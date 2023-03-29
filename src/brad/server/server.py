import asyncio
import logging
from typing import AsyncIterable, List, Tuple

import grpc
import pyodbc
import sqlglot

import brad.proto_gen.brad_pb2_grpc as brad_grpc
from brad.config.dbtype import DBType
from brad.config.file import ConfigFile
from brad.config.strings import AURORA_SEQ_COLUMN
from brad.data_sync.manager import DataSyncManager
from brad.routing import Router
from brad.routing.always_one import AlwaysOneRouter
from brad.routing.location_aware_round_robin import LocationAwareRoundRobin
from brad.routing.policy import RoutingPolicy
from brad.server.brad_interface import BradInterface
from brad.server.data_blueprint_manager import DataBlueprintManager
from brad.server.errors import QueryError
from brad.server.grpc import BradGrpc
from brad.server.session import SessionManager, SessionId
from brad.forecasting.forecaster import WorkloadForecaster
from brad.net.async_connection_acceptor import AsyncConnectionAcceptor
from brad.query_rep import QueryRep

logger = logging.getLogger(__name__)

_UPDATE_SEQ_EXPR = sqlglot.parse_one(
    "{} = DEFAULT".format(AURORA_SEQ_COLUMN)
)  # type: ignore

LINESEP = "\n".encode()


class BradServer(BradInterface):
    def __init__(self, config: ConfigFile, schema_name: str):
        self._config = config
        self._schema_name = schema_name
        self._data_blueprint_mgr = DataBlueprintManager(self._config, self._schema_name)

        # We have different routing policies for performance evaluation and
        # testing purposes.
        routing_policy = self._config.routing_policy
        if routing_policy == RoutingPolicy.Default:
            self._router: Router = LocationAwareRoundRobin(self._data_blueprint_mgr)
        elif routing_policy == RoutingPolicy.AlwaysAthena:
            self._router = AlwaysOneRouter(DBType.Athena)
        elif routing_policy == RoutingPolicy.AlwaysAurora:
            self._router = AlwaysOneRouter(DBType.Aurora)
        elif routing_policy == RoutingPolicy.AlwaysRedshift:
            self._router = AlwaysOneRouter(DBType.Redshift)
        else:
            raise RuntimeError(
                "Unsupported routing policy: {}".format(str(routing_policy))
            )

        self._sessions = SessionManager(self._config, self._schema_name)
        self._daemon_connections: List[
            Tuple[asyncio.StreamReader, asyncio.StreamWriter]
        ] = []
        self._data_sync_mgr = DataSyncManager(self._config, self._data_blueprint_mgr)
        self._timed_sync_task = None
        self._forecaster = WorkloadForecaster()

    async def serve_forever(self):
        try:
            await self.run_setup()
            daemon_acceptor = await AsyncConnectionAcceptor.create(
                host=self._config.server_interface,
                port=self._config.server_daemon_port,
                handler_function=self._handle_new_daemon_connection,
            )
            grpc_server = grpc.aio.server()
            brad_grpc.add_BradServicer_to_server(BradGrpc(self), grpc_server)
            grpc_server.add_insecure_port(
                "{}:{}".format(self._config.server_interface, self._config.server_port)
            )
            await grpc_server.start()
            logger.info("The BRAD server has successfully started.")
            logger.info("Listening on port %d.", self._config.server_port)
            await asyncio.gather(
                grpc_server.wait_for_termination(),
                daemon_acceptor.serve_forever(),
            )
        finally:
            # Not ideal, but we need to manually call this method to ensure
            # gRPC's internal shutdown process completes before we return from
            # this method.
            grpc_server.__del__()
            await self.run_teardown()
            logger.info("The BRAD server has shut down.")

    async def run_setup(self):
        await self._data_blueprint_mgr.load()
        await self._data_sync_mgr.establish_connections()
        if self._config.data_sync_period_seconds > 0:
            loop = asyncio.get_event_loop()
            self._timed_sync_task = loop.create_task(self._run_sync_periodically())

    async def run_teardown(self):
        if self._timed_sync_task is not None:
            await self._timed_sync_task.close()
            self._timed_sync_task = None

        for _, writer in self._daemon_connections:
            writer.close()
            await writer.wait_closed()
        self._daemon_connections.clear()

        await self._data_sync_mgr.close()

    def _handle_new_daemon_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        logger.debug("Accepted new daemon connection.")
        self._daemon_connections.append((reader, writer))

    async def start_session(self) -> SessionId:
        session_id, _ = await self._sessions.create_new_session()
        return session_id

    async def end_session(self, session_id: SessionId) -> None:
        await self._sessions.end_session(session_id)

    # pylint: disable-next=invalid-overridden-method
    async def run_query(
        self, session_id: SessionId, query: str
    ) -> AsyncIterable[bytes]:
        session = self._sessions.get_session(session_id)
        if session is None:
            raise QueryError("Invalid session id {}".format(str(session_id)))

        try:
            # Remove any trailing or leading whitespace.
            query = query.strip()

            # Handle internal commands separately.
            if query.startswith("BRAD_"):
                async for output in self._handle_internal_command(query):
                    yield output
                return

            query_rep = QueryRep(query)
            self._forecaster.process(query_rep)

            # 2. Select an engine for the query.
            engine_to_use = self._router.engine_for(query_rep)
            logger.debug("Routing '%s' to %s", query, engine_to_use)
            query = self._rewrite_query_if_needed(query_rep, engine_to_use)

            # 3. Actually execute the query
            connection = session.engines.get_connection(engine_to_use)
            cursor = await connection.cursor()
            try:
                await cursor.execute(query)
            except (pyodbc.ProgrammingError, pyodbc.Error) as ex:
                # Error when executing the query.
                raise QueryError.from_exception(ex)

            # Extract and return the results, if any.
            try:
                num_rows = 0
                while True:
                    row = await cursor.fetchone()
                    if row is None:
                        break
                    num_rows += 1
                    yield " | ".join(map(str, row)).encode()
                logger.debug("Responded with %d rows.", num_rows)
            except pyodbc.ProgrammingError:
                logger.debug("No rows produced.")

        except QueryError:
            # This is an expected exception. We catch and re-raise it here to
            # avoid triggering the handler below.
            raise
        except Exception as ex:
            logger.exception("Encountered unexpected exception when handling request.")
            raise QueryError.from_exception(ex)

        # NOTE: What we do here depends on the needs of the background daemon.
        if query is not None:
            try:
                for _, daemon_writer in self._daemon_connections:
                    daemon_writer.write(str(query).encode())
                    daemon_writer.write(LINESEP)
                    await daemon_writer.drain()
            except:  # pylint: disable=bare-except
                logger.exception("Exception when sending the query to the daemon.")

    def _rewrite_query_if_needed(self, query_rep: QueryRep, db_to_use: DBType) -> str:
        if db_to_use != DBType.Aurora:
            return query_rep.raw_query

        upper_query = query_rep.raw_query.upper()
        if upper_query.startswith("UPDATE"):
            # NOTE: The parser we use here is written in Python, so it is likely
            # slow. We should replace it with a C-based parser (and with a parser
            # that handles PostgreSQL SQL). But for prototype purposes (and until we
            # are sure that this is a bottleneck), this implementation is fine.
            ast = query_rep.ast()
            # Need to make sure we update the `brad_seq` column so the update
            # is picked up in the next extraction. This rewrite is specific to our
            # current extraction strategy.
            ast.expressions.append(_UPDATE_SEQ_EXPR)
            result = ast.sql()
            logger.debug("Rewrote query to '%s'", result)
            return result

        # Should ideally also handle SELECT * (to omit all brad_ prefixed
        # columns). But it is a bit cumbersome to do.
        return query_rep.raw_query

    async def _handle_internal_command(self, command: str) -> AsyncIterable[bytes]:
        """
        This method is used to handle BRAD_ prefixed "queries" (i.e., commands
        to run custom functionality like syncing data across the engines).
        """
        if command == "BRAD_SYNC;":
            logger.debug("Manually triggered a data sync.")
            await self._data_sync_mgr.run_sync()
            yield "Sync succeeded.".encode()

        elif command == "BRAD_FORECAST;":
            logger.debug("Manually triggered a workload forecast.")
            self._forecaster.forecast()
            yield "Forecast succeeded.".encode()

        else:
            yield "Unknown internal command: {}".format(command).encode()

    async def _run_sync_periodically(self):
        while True:
            await asyncio.sleep(self._config.data_sync_period_seconds)
            logger.debug("Starting an auto data sync.")
            # NOTE: This will be an async function.
            self._data_sync_mgr.run_sync()
