import asyncio
import logging
from typing import List, Optional, Tuple

import pyodbc
import sqlglot

from iohtap.config.dbtype import DBType
from iohtap.config.file import ConfigFile
from iohtap.config.routing_policy import RoutingPolicy
from iohtap.config.strings import AURORA_SEQ_COLUMN
from iohtap.config.schema import Schema
from iohtap.cost_model.always_one import AlwaysOneCostModel
from iohtap.cost_model.model import CostModel, RoundRobinCostModel
from iohtap.data_sync.manager import DataSyncManager
from iohtap.server.session import SessionManager, Session
from iohtap.forecasting.forecaster import WorkloadForecaster
from iohtap.net.async_connection_acceptor import AsyncConnectionAcceptor

logger = logging.getLogger(__name__)

_UPDATE_SEQ_EXPR = sqlglot.parse_one(
    "{} = DEFAULT".format(AURORA_SEQ_COLUMN)
)  # type: ignore

LINESEP = "\n".encode()


class IOHTAPServer:
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
        self._forecaster = WorkloadForecaster()

    async def serve_forever(self):
        try:
            self._run_setup()
            frontend_acceptor = await AsyncConnectionAcceptor.create(
                host=self._config.server_interface,
                port=self._config.server_port,
                handler_function=self._handle_request,
            )
            daemon_acceptor = await AsyncConnectionAcceptor.create(
                host=self._config.server_interface,
                port=self._config.server_daemon_port,
                handler_function=self._handle_new_daemon_connection,
            )
            logger.info("The IOHTAP server has successfully started.")
            logger.info("Listening on port %d.", self._config.server_port)
            await asyncio.gather(
                frontend_acceptor.serve_forever(), daemon_acceptor.serve_forever()
            )
        finally:
            await self._run_teardown()
            logger.info("The IOHTAP server has shut down.")

    def _run_setup(self):
        self._data_sync_mgr.establish_connections()
        _, self._the_session = self._sessions.create_new_session()
        if self._config.data_sync_period_seconds > 0:
            loop = asyncio.get_event_loop()
            loop.create_task(self._run_sync_periodically())

    async def _run_teardown(self):
        if self._the_session is not None:
            self._sessions.end_session(self._the_session.identifier)
            self._the_session = None

        for _, writer in self._daemon_connections:
            writer.close()
            await writer.wait_closed()
        self._daemon_connections.clear()

    def _handle_new_daemon_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        logger.debug("Accepted new daemon connection.")
        self._daemon_connections.append((reader, writer))

    async def _handle_request(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        # Simple protocol (the intention is that we replace this with ODBC or a
        # Postgres wire protocol compatible server):
        # - Client establishes a new connection for each request.
        # - Upon establishing a connection, it sends a SQL query terminated with
        #   a newline character.
        # - IOHTAP then routes the query to an underlying DBMS.
        # - Upon receiving the results, the server will send the results back in
        #   textual form.
        # - The server closes the connection after it finishes transmitting the
        #   results back.
        try:
            # Receive the SQL query and strip all trailing white space
            # and the semicolon.
            raw_sql_query = await reader.readline()
            sql_query = raw_sql_query.decode().strip()[:-1]
            logger.debug("Received query: %s", sql_query)

            # Route and run the request.
            cursor = await self._handle_request_internal(sql_query, writer)

            # Extract and transmit the results, if any.
            if cursor is not None:
                num_rows = 0
                for row in cursor:
                    writer.write(" | ".join(map(str, row)).encode())
                    writer.write(LINESEP)
                    num_rows += 1
                await writer.drain()
                logger.debug("Responded with %d rows.", num_rows)

        except:  # pylint: disable=bare-except
            logger.exception("Encountered exception when handling request.")
        finally:
            # 5. Close the socket to indicate the end of the result set.
            writer.close()
            await writer.wait_closed()

        # NOTE: What we do here depends on the needs of the background daemon.
        if sql_query is not None:
            try:
                for _, daemon_writer in self._daemon_connections:
                    daemon_writer.write(str(sql_query).encode())
                    daemon_writer.write(LINESEP)
                    await daemon_writer.drain()
            except:  # pylint: disable=bare-except
                logger.exception("Exception when sending the query to the daemon.")

    async def _handle_request_internal(
        self, sql_query: str, writer: asyncio.StreamWriter
    ):
        """
        The actual query handling logic should appear in this method. We keep
        this logic separate from `_handle_request()` to allow for benchmarking
        query routing overhead while excluding socket communication latency.
        """
        # Handle internal commands separately.
        if sql_query.startswith("IOHTAP_"):
            await self._handle_internal_command(sql_query, writer)
            return

        self._forecaster.process(sql_query)

        # 2. Predict which DBMS to use.
        run_times = self._cost_model.predict_run_time(sql_query)
        db_to_use, _ = run_times.min_time_ms()
        logger.debug("Routing '%s' to %s", sql_query, db_to_use)
        sql_query = self._rewrite_query_if_needed(sql_query, db_to_use)

        # 3. Actually execute the query
        assert self._the_session is not None
        connection = self._the_session.engines.get_connection(db_to_use)
        cursor = connection.cursor()
        try:
            cursor.execute(sql_query)
        except pyodbc.ProgrammingError as ex:
            # Error when executing the query.
            writer.write(str(ex).encode())
            writer.write(LINESEP)
            logger.debug("Query failed with exception %s", str(ex))
            await writer.drain()
            return

        # 4. Return the cursor for results extraction.
        return cursor

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
            # Need to make sure we update the `iohtap_seq` column so the update
            # is picked up in the next extraction. This rewrite is specific to our
            # current extraction strategy.
            parsed.expressions.append(_UPDATE_SEQ_EXPR)
            result = parsed.sql()
            logger.debug("Rewrote query to '%s'", result)
            return result

        # Should ideally also handle SELECT * (to omit all iohtap_ prefixed
        # columns). But it is a bit cumbersome to do.
        return sql_query

    async def _handle_internal_command(
        self, command: str, writer: asyncio.StreamWriter
    ):
        """
        This method is used to handle IOHTAP_ prefixed "queries" (i.e., commands
        to run custom functionality like syncing data across the engines).
        """
        if command == "IOHTAP_SYNC":
            logger.debug("Manually triggered a data sync.")
            self._data_sync_mgr.run_sync()
            writer.write("Sync succeeded.".encode())
        elif command == "IOHTAP_FORECAST":
            logger.debug("Manually triggered a workload forecast.")
            self._forecaster.forecast()
            writer.write("Forecast succeeded.".encode())
        else:
            writer.write("Unknown internal command:".encode())
            writer.write(command.encode())
        writer.write(LINESEP)
        await writer.drain()

    async def _run_sync_periodically(self):
        while True:
            await asyncio.sleep(self._config.data_sync_period_seconds)
            logger.debug("Starting an auto data sync.")
            # NOTE: This will be an async function.
            self._data_sync_mgr.run_sync()
