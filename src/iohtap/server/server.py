import logging
import pyodbc
import socket
import sqlglot
from concurrent.futures import ThreadPoolExecutor
from typing import List, TextIO

from iohtap.config.dbtype import DBType
from iohtap.config.file import ConfigFile
from iohtap.config.strings import AURORA_SEQ_COLUMN
from iohtap.config.schema import Schema
from iohtap.cost_model.model import CostModel
from iohtap.data_sync.manager import DataSyncManager
from iohtap.server.db_connection_manager import DBConnectionManager
from iohtap.forecasting.forecaster import WorkloadForecaster
from iohtap.net.connection_acceptor import ConnectionAcceptor
from iohtap.utils.timer_trigger import TimerTrigger

logger = logging.getLogger(__name__)

_UPDATE_SEQ_EXPR = sqlglot.parse_one(
    "{} = DEFAULT".format(AURORA_SEQ_COLUMN)
)  # type: ignore


class IOHTAPServer:
    def __init__(self, config: ConfigFile, schema: Schema):
        self._config = config
        self._schema = schema
        self._cost_model = CostModel()
        self._dbs = DBConnectionManager(self._config)
        self._connection_acceptor = ConnectionAcceptor(
            self._config.server_interface,
            self._config.server_port,
            self._on_new_connection,
        )
        self._daemon_connection_acceptor = ConnectionAcceptor(
            self._config.server_interface,
            self._config.server_daemon_port,
            self._on_new_daemon_connection,
        )
        self._daemon_connections: List[TextIO] = []
        # NOTE: The data sync should be invoked from the daemon. We put it here
        # for convenience (until we implement a more robust client/daemon
        # interaction).
        self._data_sync_mgr = DataSyncManager(self._config, self._schema, self._dbs)
        self._auto_sync_timer = (
            TimerTrigger(
                period_s=self._config.data_sync_period_seconds,
                to_run=self._schedule_sync,
            )
            if self._config.data_sync_period_seconds > 0
            else None
        )
        self._main_executor = ThreadPoolExecutor(max_workers=1)
        self._forecaster = WorkloadForecaster()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def start(self):
        self._connection_acceptor.start()
        self._daemon_connection_acceptor.start()
        if self._auto_sync_timer is not None:
            self._auto_sync_timer.start()
        logger.info("The IOHTAP server has successfully started.")
        logger.info("Listening on port %d.", self._config.server_port)

    def stop(self):
        def shutdown():
            if self._auto_sync_timer is not None:
                self._auto_sync_timer.stop()
            self._daemon_connection_acceptor.stop()
            self._connection_acceptor.stop()
            for daemon in self._daemon_connections:
                daemon.close()
            self._daemon_connections.clear()

        self._main_executor.submit(shutdown).result()
        self._main_executor.shutdown()
        logger.info("The IOHTAP server has shut down.")

    def _on_new_connection(self, client_socket, _addr_info):
        self._main_executor.submit(self._handle_request, client_socket)

    def _on_new_daemon_connection(self, daemon_socket, _addr_info):
        self._main_executor.submit(self._register_daemon, daemon_socket)

    def _handle_request(self, client_socket: socket.socket):
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
            with client_socket.makefile("rw") as io:
                # 1. Receive the SQL query and strip all trailing white space
                #    and the semicolon.
                sql_query = io.readline().strip()[:-1]
                logger.debug("Received query: %s", sql_query)
                self._forecaster.process(sql_query)

                # Handle internal commands separately.
                if sql_query.startswith("IOHTAP_"):
                    self._handle_internal_command(sql_query, io)
                    return

                # 2. Predict which DBMS to use.
                run_times = self._cost_model.predict_run_time(sql_query)
                db_to_use, _ = run_times.min_time_ms()
                logger.debug("Routing '%s' to %s", sql_query, db_to_use)
                sql_query = self._rewrite_query_if_needed(sql_query, db_to_use)

                # 3. Actually execute the query
                connection = self._dbs.get_connection(db_to_use)
                cursor = connection.cursor()
                try:
                    cursor.execute(sql_query)
                except pyodbc.ProgrammingError as ex:
                    # Error when executing the query.
                    print(str(ex), file=io, flush=True)
                    logger.debug("Query failed with exception %s", str(ex))
                    return

                # 4. Extract and transmit the results.
                num_rows = 0
                for row in cursor:
                    print(" | ".join(map(str, row)), file=io)
                    num_rows += 1
                logger.debug("Responded with %d rows.", num_rows)

        except:  # pylint: disable=bare-except
            logger.exception("Encountered exception when handling request.")
        finally:
            # 5. Close the socket to indicate the end of the result set.
            client_socket.close()

        # NOTE: What we do here depends on the needs of the background daemon.
        if sql_query is not None:
            try:
                for daemon in self._daemon_connections:
                    print(str(sql_query), file=daemon, flush=True)
            except:  # pylint: disable=bare-except
                logger.exception("Exception when sending the query to the daemon.")

    def _register_daemon(self, daemon_socket: socket.socket):
        self._daemon_connections.append(daemon_socket.makefile("w"))

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

    def _handle_internal_command(self, command: str, io: TextIO):
        """
        This method is used to handle IOHTAP_ prefixed "queries" (i.e., commands
        to run custom functionality like syncing data across the engines).
        """
        if command == "IOHTAP_SYNC":
            logger.debug("Manually triggered a data sync.")
            self._data_sync_mgr.run_sync()
            print("Sync succeeded.", file=io, flush=True)
        elif command == "IOHTAP_FORECAST":
            logger.debug("Manually triggered a workload forecast.")
            self._forecaster.forecast()
            print("Forecast succeeded.", file=io, flush=True)
        else:
            print("Unknown internal command:", command, file=io, flush=True)

    def _schedule_sync(self):
        # This method is called by the timer thread. We want it to wait until
        # the sync completes (in case the sync takes longer than the timer
        # period).
        logger.debug("Scheduling an auto data sync.")
        self._main_executor.submit(self._data_sync_mgr.run_sync).result()
