import asyncio
import logging
from typing import AsyncIterable, List, Optional, Tuple

import grpc
import pyodbc
import sqlglot

import brad.grpc_gen.brad_pb2_grpc as brad_grpc
from brad.config.dbtype import DBType
from brad.config.file import ConfigFile
from brad.config.routing_policy import RoutingPolicy
from brad.config.strings import AURORA_SEQ_COLUMN
from brad.config.schema import Schema
from brad.cost_model.always_one import AlwaysOneCostModel
from brad.cost_model.model import CostModel, RoundRobinCostModel
from brad.data_sync.manager import DataSyncManager
from brad.server.brad_interface import BradInterface
from brad.server.errors import QueryError
from brad.server.grpc import BradGrpc
from brad.server.session import SessionManager, Session, SessionId
from brad.forecasting.forecaster import WorkloadForecaster
from brad.net.async_connection_acceptor import AsyncConnectionAcceptor

logger = logging.getLogger(__name__)

_UPDATE_SEQ_EXPR = sqlglot.parse_one(
    "{} = DEFAULT".format(AURORA_SEQ_COLUMN)
)  # type: ignore

LINESEP = "\n".encode()


class BradServer(BradInterface):
    def __init__(self, config: ConfigFile, schema: Schema):
        self._config = config
        self._schema = schema

        # We have different routing policies for performance evaluation and
        # testing purposes.
        routing_policy = self._config.routing_policy
        if routing_policy == RoutingPolicy.Default:
            self._cost_model: CostModel = RoundRobinCostModel()
        elif routing_policy == RoutingPolicy.AlwaysAthena:
            self._cost_model = AlwaysOneCostModel(DBType.Athena)
        elif routing_policy == RoutingPolicy.AlwaysAurora:
            self._cost_model = AlwaysOneCostModel(DBType.Aurora)
        elif routing_policy == RoutingPolicy.AlwaysRedshift:
            self._cost_model = AlwaysOneCostModel(DBType.Redshift)
        else:
            raise RuntimeError(
                "Unsupported routing policy: {}".format(str(routing_policy))
            )

        self._sessions = SessionManager(self._config)
        # NOTE: This is temporary - we will support multiple concurrent sessions.
        self._the_session: Optional[Session] = None

        self._daemon_connections: List[
            Tuple[asyncio.StreamReader, asyncio.StreamWriter]
        ] = []
        self._data_sync_mgr = DataSyncManager(self._config, self._schema)
        self._timed_sync_task = None
        self._forecaster = WorkloadForecaster()

    async def serve_forever(self):
        try:
            await self._run_setup()
            frontend_acceptor = await AsyncConnectionAcceptor.create(
                host=self._config.server_interface,
                port=self._config.server_port,
                handler_function=self._handle_raw_request,
            )
            daemon_acceptor = await AsyncConnectionAcceptor.create(
                host=self._config.server_interface,
                port=self._config.server_daemon_port,
                handler_function=self._handle_new_daemon_connection,
            )
            grpc_server = grpc.aio.server()
            brad_grpc.add_BradServicer_to_server(BradGrpc(self), grpc_server)
            grpc_server.add_insecure_port(str(self._config.server_port + 1))
            await grpc_server.start()
            logger.info("The BRAD server has successfully started.")
            logger.info("Listening on port %d.", self._config.server_port)
            await asyncio.gather(
                frontend_acceptor.serve_forever(),
                daemon_acceptor.serve_forever(),
                grpc_server.wait_for_termination(),
            )
        finally:
            await self._run_teardown()
            logger.info("The BRAD server has shut down.")

    async def _run_setup(self):
        await self._data_sync_mgr.establish_connections()
        _, self._the_session = await self._sessions.create_new_session()
        if self._config.data_sync_period_seconds > 0:
            loop = asyncio.get_event_loop()
            self._timed_sync_task = loop.create_task(self._run_sync_periodically())

    async def _run_teardown(self):
        if self._timed_sync_task is not None:
            await self._timed_sync_task.close()
            self._timed_sync_task = None

        if self._the_session is not None:
            await self._sessions.end_session(self._the_session.identifier)
            self._the_session = None

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

    async def _handle_raw_request(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        # NOTE: This exists for transition purposes - it will be removed.
        # Simple protocol (the intention is that we replace this with ODBC or a
        # Postgres wire protocol compatible server):
        # - Client establishes a new connection for each request.
        # - Upon establishing a connection, it sends a SQL query terminated with
        #   a newline character.
        # - BRAD then routes the query to an underlying DBMS.
        # - Upon receiving the results, the server will send the results back in
        #   textual form.
        # - The server closes the connection after it finishes transmitting the
        #   results back.
        try:
            # Receive the SQL query and strip all trailing white space
            # and the semicolon.
            raw_sql_query = await reader.readline()
            sql_query = raw_sql_query.decode().strip()[:-1]
            logger.debug("Received query from raw interface: %s", sql_query)
            assert self._the_session is not None
            session_id = self._the_session.identifier
            async for out_row in self.run_query(session_id, sql_query):
                writer.write(out_row)
                writer.write(LINESEP)
            await writer.drain()

        except QueryError as ex:
            writer.write(str(ex).encode())
            await writer.drain()
        except:  # pylint: disable=bare-except
            logger.exception(
                "Encountered unexpected exception when handling raw request."
            )
        finally:
            writer.close()
            await writer.wait_closed()

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
            # Handle internal commands separately.
            if query.startswith("BRAD_"):
                async for output in self._handle_internal_command(query):
                    yield output
                return

            self._forecaster.process(query)

            # 2. Predict which DBMS to use.
            run_times = self._cost_model.predict_run_time(query)
            db_to_use, _ = run_times.min_time_ms()
            logger.debug("Routing '%s' to %s", query, db_to_use)
            query = self._rewrite_query_if_needed(query, db_to_use)

            # 3. Actually execute the query
            assert self._the_session is not None
            connection = self._the_session.engines.get_connection(db_to_use)
            cursor = await connection.cursor()
            try:
                await cursor.execute(query)
            except pyodbc.ProgrammingError as ex:
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

    def _rewrite_query_if_needed(self, sql_query: str, db_to_use: DBType) -> str:
        if db_to_use != DBType.Aurora:
            return sql_query

        upper_query = sql_query.upper()
        if upper_query.startswith("UPDATE"):
            # NOTE: The parser we use here is written in Python, so it is likely
            # slow. We should replace it with a C-based parser (and with a parser
            # that handles PostgreSQL SQL). But for prototype purposes (and until we
            # are sure that this is a bottleneck), this implementation is fine.
            parsed = sqlglot.parse_one(sql_query)  # type: ignore
            # Need to make sure we update the `brad_seq` column so the update
            # is picked up in the next extraction. This rewrite is specific to our
            # current extraction strategy.
            parsed.expressions.append(_UPDATE_SEQ_EXPR)
            result = parsed.sql()
            logger.debug("Rewrote query to '%s'", result)
            return result

        # Should ideally also handle SELECT * (to omit all brad_ prefixed
        # columns). But it is a bit cumbersome to do.
        return sql_query

    async def _handle_internal_command(self, command: str) -> AsyncIterable[bytes]:
        """
        This method is used to handle BRAD_ prefixed "queries" (i.e., commands
        to run custom functionality like syncing data across the engines).
        """
        if command == "BRAD_SYNC":
            logger.debug("Manually triggered a data sync.")
            await self._data_sync_mgr.run_sync()
            yield "Sync succeeded.".encode()

        elif command == "BRAD_FORECAST":
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
