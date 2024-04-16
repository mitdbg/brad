import asyncio
import json
import logging
import random
import time
import ssl
import multiprocessing as mp
import redshift_connector.error as redshift_errors
import psycopg
import struct
from typing import AsyncIterable, Optional, Dict, Any
from datetime import timedelta
from ddsketch import DDSketch

import grpc
import pyodbc

import brad.proto_gen.brad_pb2_grpc as brad_grpc

from brad.asset_manager import AssetManager
from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.connection.connection import ConnectionFailed
from brad.daemon.monitor import Monitor
from brad.daemon.messages import (
    ShutdownFrontEnd,
    Sentinel,
    MetricsReport,
    InternalCommandRequest,
    InternalCommandResponse,
    NewBlueprint,
    NewBlueprintAck,
)
from brad.front_end.brad_interface import BradInterface
from brad.front_end.errors import QueryError
from brad.front_end.grpc import BradGrpc
from brad.front_end.session import SessionManager, SessionId, Session
from brad.front_end.watchdog import Watchdog
from brad.provisioning.directory import Directory
from brad.query_rep import QueryRep
from brad.routing.abstract_policy import AbstractRoutingPolicy
from brad.routing.always_one import AlwaysOneRouter
from brad.routing.rule_based import RuleBased
from brad.routing.policy import RoutingPolicy
from brad.routing.router import Router
from brad.routing.tree_based.forest_policy import ForestPolicy
from brad.row_list import RowList
from brad.utils import log_verbose, create_custom_logger
from brad.utils.counter import Counter
from brad.utils.json_decimal_encoder import DecimalEncoder
from brad.utils.mailbox import Mailbox
from brad.utils.rand_exponential_backoff import RandomizedExponentialBackoff
from brad.utils.run_time_reservoir import RunTimeReservoir
from brad.utils.time_periods import universal_now
from brad.workload_logging.epoch_file_handler import EpochFileHandler

logger = logging.getLogger(__name__)

LINESEP = "\n".encode()


class BradFrontEnd(BradInterface):
    @staticmethod
    def native_server_is_supported() -> bool:
        """
        If the native pybind_brad_server module built using Arrow Flight SQL
        exists, this function will return True. Otherwise, it returns False.
        """
        try:
            # pylint: disable-next=import-error,no-name-in-module,unused-import
            import brad.native.pybind_brad_server as brad_server

            return True
        except ImportError:
            return False

    def __init__(
        self,
        fe_index: int,
        config: ConfigFile,
        schema_name: str,
        path_to_system_config: str,
        debug_mode: bool,
        initial_directory: Directory,
        input_queue: mp.Queue,
        output_queue: mp.Queue,
    ):
        if BradFrontEnd.native_server_is_supported():
            from brad.front_end.flight_sql_server import BradFlightSqlServer

            self._flight_sql_server: Optional[BradFlightSqlServer] = (
                BradFlightSqlServer(
                    host="0.0.0.0",
                    port=31337,
                    callback=self._handle_query_from_flight_sql,
                )
            )
            self._flight_sql_server_session_id: Optional[SessionId] = None
        else:
            self._flight_sql_server = None

        self._main_thread_loop: Optional[AbstractEventLoop] = None

        self._fe_index = fe_index
        self._config = config
        self._schema_name = schema_name
        self._debug_mode = debug_mode

        # Used for IPC with the daemon. Eventually we will use RPC to
        # communicate with the daemon. But there's currently no need for
        # something fancy here.
        # Used for messages sent from the daemon to this front end server.
        self._input_queue = input_queue
        # Used for messages sent from this front end server to the daemon.
        self._output_queue = output_queue

        self._assets = AssetManager(self._config)
        self._blueprint_mgr = BlueprintManager(
            self._config,
            self._assets,
            self._schema_name,
            # This is provided by the daemon. We want to avoid hitting the AWS
            # cluster metadata APIs when starting up the front end(s).
            initial_directory=initial_directory,
        )
        self._path_to_system_config = path_to_system_config
        self._monitor: Optional[Monitor] = None

        # Set up query logger
        self._qlogger = logging.getLogger("queries")
        self._qlogger.setLevel(logging.INFO)
        self._qhandler = EpochFileHandler(
            self._fe_index,
            self._config.local_logs_path,
            self._config.epoch_length,
            self._config.s3_logs_bucket,
            self._config.s3_logs_path,
            self._config.txn_log_prob,
        )
        self._qhandler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(message)s")
        self._qhandler.setFormatter(formatter)
        self._qlogger.addHandler(self._qhandler)
        self._qlogger.propagate = False  # Avoids printing to stdout

        # Used to track query performance.
        self._query_run_times = RunTimeReservoir[float](
            self._config.front_end_query_latency_buffer_size
        )

        self._routing_policy_override = self._config.routing_policy
        # This is set up as the front end starts up.
        self._router: Optional[Router] = None
        self._sessions = SessionManager(
            self._config, self._blueprint_mgr, self._schema_name
        )
        self._daemon_messages_task: Optional[asyncio.Task[None]] = None

        # Number of transactions that completed.
        self._transaction_end_counter = Counter()  # pylint: disable=global-statement
        self._reset_latency_sketches()
        self._brad_metrics_reporting_task: Optional[asyncio.Task[None]] = None

        # Used to manage `BRAD_` requests that need to be sent to the daemon for
        # execution. We only allow one such request to be in flight at any time
        # (to simplify the logic here, since we only need it for completeness).
        self._daemon_request_mailbox = Mailbox[str, RowList](
            do_send_msg=self._send_daemon_request
        )

        # Used to close a logging epoch when needed.
        self._qlogger_refresh_task: Optional[asyncio.Task[None]] = None

        # Used to re-establish engine connections.
        self._reestablish_connections_task: Optional[asyncio.Task[None]] = None

        # Used for logging transient errors that are too verbose.
        main_log_file = config.front_end_log_file(fe_index)
        if main_log_file is not None:
            verbose_log_file = (
                main_log_file.parent / f"brad_front_end_verbose_{fe_index}.log"
            )
            self._verbose_logger: Optional[logging.Logger] = create_custom_logger(
                "fe_verbose", str(verbose_log_file)
            )
            self._verbose_logger.info("Verbose logger enabled.")
        else:
            self._verbose_logger = None

        # Used for debug purposes.
        # We print the system state if the front end becomes unresponsive for >= 5 mins.
        self._watchdog = Watchdog(
            check_period=timedelta(minutes=1), take_action_after=timedelta(minutes=5)
        )
        self._ping_watchdog_task: Optional[asyncio.Task[None]] = None

        self._is_stub_mode = self._config.stub_mode_path is not None

    def _handle_query_from_flight_sql(self, query: str) -> RowList:
        assert self._flight_sql_server_session_id is not None

        future = asyncio.run_coroutine_threadsafe(
            self._run_query_impl(self._flight_sql_server_session_id, query, {}),
            self._main_thread_loop,
        )
        row_result = future.result()

        return row_result

    async def serve_forever(self):
        await self._run_setup()

        # Start FlightSQL server
        if self._flight_sql_server is not None:
            self._flight_sql_server_session_id = await self.start_session()
            self._flight_sql_server.start()

        try:
            grpc_server = grpc.aio.server()
            brad_grpc.add_BradServicer_to_server(BradGrpc(self), grpc_server)
            port_to_use = self._config.front_end_port + self._fe_index
            grpc_server.add_insecure_port(
                "{}:{}".format(self._config.front_end_interface, port_to_use)
            )
            await grpc_server.start()
            logger.info("The BRAD front end has successfully started.")
            logger.info("Listening on port %d.", port_to_use)

            # N.B. If we need the Monitor, we should call `run_forever()` here.
            await grpc_server.wait_for_termination()
        finally:
            # Not ideal, but we need to manually call this method to ensure
            # gRPC's internal shutdown process completes before we return from
            # this method.
            grpc_server.__del__()
            await self._run_teardown()
            logger.debug("BRAD front end _run_teardown() complete.")

    async def _run_setup(self) -> None:
        self._main_thread_loop = asyncio.get_running_loop()

        # The directory will have been populated by the daemon.
        await self._blueprint_mgr.load(skip_directory_refresh=True)
        logger.info("Using blueprint: %s", self._blueprint_mgr.get_blueprint())

        if self._monitor is not None:
            self._monitor.set_up_metrics_sources()
            await self._monitor.fetch_latest()

        await self._set_up_router()

        # Start the metrics reporting task.
        self._brad_metrics_reporting_task = asyncio.create_task(
            self._report_metrics_to_daemon()
        )

        # Used to handle messages from the daemon.
        self._daemon_messages_task = asyncio.create_task(self._read_daemon_messages())

        if not self._is_stub_mode:
            self._qlogger_refresh_task = asyncio.create_task(self._refresh_qlogger())
        self._watchdog.start(self._main_thread_loop)
        self._ping_watchdog_task = asyncio.create_task(self._ping_watchdog())

    async def _set_up_router(self) -> None:
        # We have different routing policies for performance evaluation and
        # testing purposes.
        blueprint = self._blueprint_mgr.get_blueprint()

        if self._routing_policy_override == RoutingPolicy.Default:
            # No override - use the blueprint's policy.
            self._router = Router.create_from_blueprint(blueprint)
            logger.info("Using blueprint-provided routing policy.")

        else:
            if self._routing_policy_override == RoutingPolicy.AlwaysAthena:
                definite_policy: AbstractRoutingPolicy = AlwaysOneRouter(Engine.Athena)
            elif self._routing_policy_override == RoutingPolicy.AlwaysAurora:
                definite_policy = AlwaysOneRouter(Engine.Aurora)
            elif self._routing_policy_override == RoutingPolicy.AlwaysRedshift:
                definite_policy = AlwaysOneRouter(Engine.Redshift)
            elif self._routing_policy_override == RoutingPolicy.RuleBased:
                # TODO: If we need metrics, re-create the monitor here. It's
                # easier to not have it created.
                definite_policy = RuleBased()
            elif (
                self._routing_policy_override == RoutingPolicy.ForestTablePresence
                or self._routing_policy_override == RoutingPolicy.ForestTableSelectivity
            ):
                definite_policy = await ForestPolicy.from_assets(
                    self._schema_name,
                    self._routing_policy_override,
                    self._assets,
                )
            else:
                raise RuntimeError(
                    f"Unsupported routing policy override: {self._routing_policy_override}"
                )
            logger.info(
                "Using routing policy override: %s", self._routing_policy_override.name
            )
            self._router = Router.create_from_definite_policy(
                definite_policy, blueprint.table_locations_bitmap()
            )

        self._router.log_policy()

    async def _run_teardown(self):
        logger.debug("Starting BRAD front end _run_teardown()")

        # Shutdown FlightSQL server
        if self._flight_sql_server is not None:
            self._flight_sql_server.stop()

        await self._sessions.end_all_sessions()

        # Important for unblocking our message reader thread.
        self._input_queue.put(Sentinel(self._fe_index))

        if self._daemon_messages_task is not None:
            self._daemon_messages_task.cancel()
            self._daemon_messages_task = None

        if self._brad_metrics_reporting_task is not None:
            self._brad_metrics_reporting_task.cancel()
            self._brad_metrics_reporting_task = None

        if self._qlogger_refresh_task is not None:
            self._qlogger_refresh_task.cancel()
            self._qlogger_refresh_task = None

        self._watchdog.stop()
        if self._ping_watchdog_task is not None:
            self._ping_watchdog_task.cancel()
            self._ping_watchdog_task = None

    async def start_session(self) -> SessionId:
        rand_backoff = None
        while True:
            try:
                session_id, _ = await self._sessions.create_new_session()
                if self._verbose_logger is not None:
                    self._verbose_logger.info(
                        "New session started %d", session_id.value()
                    )
                return session_id
            except ConnectionFailed:
                if rand_backoff is None:
                    rand_backoff = RandomizedExponentialBackoff(
                        max_retries=20, base_delay_s=0.5, max_delay_s=10.0
                    )
                time_to_wait = rand_backoff.wait_time_s()
                if time_to_wait is None:
                    logger.exception(
                        "Failed to start a new session due to a repeated "
                        "connection failure (10 retries)."
                    )
                    raise
                await asyncio.sleep(time_to_wait)
                # Defensively refresh the blueprint and directory before
                # retrying. Maybe we are getting outdated endpoint information
                # from AWS.
                await self._blueprint_mgr.load()

    async def end_session(self, session_id: SessionId) -> None:
        await self._sessions.end_session(session_id)
        if self._verbose_logger is not None:
            self._verbose_logger.info("Session ended %d", session_id.value())

    # pylint: disable-next=invalid-overridden-method
    async def run_query(
        self, session_id: SessionId, query: str, debug_info: Dict[str, Any]
    ) -> AsyncIterable[bytes]:
        results = await self._run_query_impl(session_id, query, debug_info)
        for row in results:
            yield (" | ".join(map(str, row))).encode()

    async def run_query_json(
        self, session_id: SessionId, query: str, debug_info: Dict[str, Any]
    ) -> str:
        results = await self._run_query_impl(session_id, query, debug_info)
        return json.dumps(results, cls=DecimalEncoder, default=str)

    async def _run_query_impl(
        self, session_id: SessionId, query: str, debug_info: Dict[str, Any]
    ) -> RowList:
        session = self._sessions.get_session(session_id)
        if session is None:
            raise QueryError(
                "Invalid session id {}".format(str(session_id)), is_transient=False
            )

        try:
            # Remove any trailing or leading whitespace. Remove the trailing
            # semicolon if it exists.
            # NOTE: BRAD does not yet support having multiple
            # semicolon-separated queries in one request.
            query = self._clean_query_str(query)

            # Handle internal commands separately.
            if query.startswith("BRAD_"):
                return await self._handle_internal_command(session, query, debug_info)

            # 2. Select an engine for the query.
            query_rep = QueryRep(query)
            if query_rep.is_transaction_start():
                session.set_in_transaction(True)

            if query.startswith("SET SESSION"):
                # Support for setting transaction isolation level (temporary).
                engine_to_use = Engine.Aurora
            else:
                assert self._router is not None
                engine_to_use = await self._router.engine_for(query_rep, session)

            log_verbose(
                logger,
                "[S%d] Routing '%s' to %s",
                session_id.value(),
                query,
                engine_to_use,
            )
            debug_info["executor"] = engine_to_use

            # 3. Actually execute the query.
            try:
                transactional_query: bool = (
                    session.in_transaction or query_rep.is_data_modification_query()
                )
                if transactional_query:
                    connection = session.engines.get_connection(engine_to_use)
                    cursor = connection.cursor_sync()
                    start = universal_now()
                    if query_rep.is_transaction_start():
                        session.set_txn_start_timestamp(start)
                    # Using execute_sync() is lower overhead than the async
                    # interface. For transactions, we won't necessarily need the
                    # async interface.
                    cursor.execute_sync(query_rep.raw_query)
                else:
                    connection = session.engines.get_reader_connection(engine_to_use)
                    cursor = connection.cursor_sync()
                    start = universal_now()
                    await cursor.execute(query_rep.raw_query)
                end = universal_now()
            except (
                pyodbc.ProgrammingError,
                pyodbc.Error,
                pyodbc.OperationalError,
                redshift_errors.InterfaceError,
                ssl.SSLEOFError,  # Occurs during Redshift restarts.
                IndexError,  # Occurs during Redshift restarts.
                struct.error,  # Occurs during Redshift restarts.
                psycopg.Error,
                psycopg.OperationalError,
                psycopg.ProgrammingError,
            ) as ex:
                is_transient_error = False
                if connection.is_connection_lost_error(ex):
                    connection.mark_connection_lost()
                    self._schedule_reestablish_connections()
                    is_transient_error = True
                    # N.B. We still pass the error to the client. The client
                    # should retry the query (later on we can add more graceful
                    # handling here).

                # Error when executing the query.
                raise QueryError.from_exception(ex, is_transient_error)

            # We keep track of transactional state after executing the query in
            # case the query failed.
            if query_rep.is_transaction_start():
                session.set_in_transaction(in_txn=True)

            is_transaction_end = query_rep.is_transaction_end()
            if is_transaction_end:
                session.set_in_transaction(in_txn=False)
                self._transaction_end_counter.bump()

            # Decide whether to log the query.
            run_time_s = end - start
            if not transactional_query or (random.random() < self._config.txn_log_prob):
                if not self._is_stub_mode:
                    # Skip logging the query when running in stub mode.
                    self._qlogger.info(
                        f"{end.strftime('%Y-%m-%d %H:%M:%S,%f')} INFO Query: {query} "
                        f"Engine: {engine_to_use.value} "
                        f"Duration (s): {run_time_s.total_seconds()} "
                        f"IsTransaction: {transactional_query}"
                    )
                run_time_s_float = run_time_s.total_seconds()
                if not transactional_query:
                    self._query_latency_sketch.add(run_time_s_float)
                elif is_transaction_end:
                    # We want to record the duration of the entire transaction
                    # (not just one query in the transaction).
                    self._txn_latency_sketch.add(
                        (end - session.txn_start_timestamp()).total_seconds()
                    )

            # Extract and return the results, if any.
            try:
                # Using `fetchall_sync()` is lower overhead than the async interface.
                results = [tuple(row) for row in cursor.fetchall_sync()]
                log_verbose(logger, "Responded with %d rows.", len(results))
                return results
            except (pyodbc.ProgrammingError, psycopg.ProgrammingError):
                log_verbose(logger, "No rows produced.")
                return []
            except (
                pyodbc.Error,
                pyodbc.OperationalError,
                psycopg.Error,
                psycopg.OperationalError,
            ) as ex:
                is_transient_error = False
                if connection.is_connection_lost_error(ex):
                    connection.mark_connection_lost()
                    self._schedule_reestablish_connections()
                    is_transient_error = True

                raise QueryError.from_exception(ex, is_transient_error)

        except QueryError as ex:
            # This is an expected exception. We catch and re-raise it here to
            # avoid triggering the handler below.
            logger.debug("Query error: %s", repr(ex))
            if self._verbose_logger is not None:
                if ex.is_transient():
                    self._verbose_logger.exception("Transient error")
                else:
                    self._verbose_logger.exception("Non-transient error")
            raise
        except Exception as ex:
            logger.exception("Encountered unexpected exception when handling request.")
            raise QueryError.from_exception(ex)

    async def _handle_internal_command(
        self, session: Session, command_raw: str, debug_info: Dict[str, Any]
    ) -> RowList:
        """
        This method is used to handle BRAD_ prefixed "queries" (i.e., commands
        to run custom functionality like syncing data across the engines).
        """
        command = command_raw.upper()
        if not command.startswith("BRAD_INSPECT_WORKLOAD"):
            debug_info["not_tabular"] = True

        if (
            command == "BRAD_SYNC"
            or command == "BRAD_EXPLAIN_SYNC_STATIC"
            or command == "BRAD_EXPLAIN_SYNC"
            or command.startswith("BRAD_INSPECT_WORKLOAD")
            or command.startswith("BRAD_RUN_PLANNER")
            or command.startswith("BRAD_MODIFY_REDSHIFT")
            or command.startswith("BRAD_USE_PRESET_BP")
        ):
            if self._daemon_request_mailbox.is_active():
                return [
                    (
                        "Another internal command is pending. "
                        "Please wait for it to complete first.",
                    )
                ]

            if command == "BRAD_SYNC":
                logger.debug("Manually requested a data sync.")

            # Send the command to the daemon for execution.
            return await self._daemon_request_mailbox.send_recv(command)

        elif command == "BRAD_NOOP":
            # Used to measure the overhead of accessing BRAD.
            return [("OK",)]

        elif command.startswith("BRAD_RUN_ON"):
            # BRAD_RUN_ON <engine> <query or command>
            # This is useful for commands used to set up experiments (e.g.,
            # running ANALYZE).
            parts = command_raw.split(" ")
            try:
                engine = Engine.from_str(parts[1])
            except ValueError as ex:
                return [(str(ex),)]

            query = " ".join(parts[2:])
            if len(query) == 0:
                return [("Empty query/command.",)]

            try:
                connection = session.engines.get_connection(engine)
                cursor = await connection.cursor()
                logger.debug("Requested to run on %s: %s", str(engine), query)
                await cursor.execute(query)
            except RuntimeError as ex:
                return [(str(ex),)]

            # Extract and return the results, if any.
            try:
                return [tuple(row) for row in cursor.fetchall_sync()]
            except pyodbc.ProgrammingError:
                return []

        else:
            return [("Unknown internal command: {}".format(command),)]

    async def _read_daemon_messages(self) -> None:
        assert self._input_queue is not None
        loop = asyncio.get_running_loop()
        while True:
            try:
                message = await loop.run_in_executor(None, self._input_queue.get)
                if message.fe_index != self._fe_index:
                    logger.warning(
                        "Received message with invalid front end index. Expected %d. Received %d.",
                        self._fe_index,
                        message.fe_index,
                    )
                    continue

                if isinstance(message, ShutdownFrontEnd):
                    logger.debug("The BRAD front end is initiating a shut down...")
                    loop.create_task(_orchestrate_shutdown())
                    break

                elif isinstance(message, InternalCommandResponse):
                    if not self._daemon_request_mailbox.is_active():
                        logger.warning(
                            "Received an internal command response but no one was waiting for it: %s",
                            message,
                        )
                        continue
                    self._daemon_request_mailbox.on_new_message(message.response)

                elif isinstance(message, NewBlueprint):
                    logger.info(
                        "Received notification to update to blueprint version %d",
                        message.version,
                    )
                    # This refreshes any cached state that depends on the old blueprint.
                    await self._run_blueprint_update(
                        message.version, message.updated_directory
                    )
                    # Tell the daemon that we have updated.
                    self._output_queue.put(
                        NewBlueprintAck(self._fe_index, message.version), block=False
                    )
                    logger.info(
                        "Acknowledged update to blueprint version %d", message.version
                    )

                else:
                    logger.info("Received message from the daemon: %s", message)
            except Exception as ex:
                if not isinstance(ex, asyncio.CancelledError):
                    logger.exception(
                        "Unexpected error when handling message from the daemon."
                    )

    async def _report_metrics_to_daemon(self) -> None:
        try:
            period_start = time.time()

            # We want to stagger the reports across the front ends to avoid
            # overwhelming the daemon.
            await asyncio.sleep(0.1 * self._fe_index)

            while True:
                # Ideally we adjust for delays here too.
                await asyncio.sleep(
                    self._config.front_end_metrics_reporting_period_seconds
                )

                txn_value = self._transaction_end_counter.value()
                period_end = time.time()
                self._transaction_end_counter.reset()
                elapsed_time_s = period_end - period_start

                # If the input queue is full, we just drop this message.
                sampled_thpt = txn_value / elapsed_time_s
                metrics_report = MetricsReport.from_data(
                    self._fe_index,
                    sampled_thpt,
                    self._txn_latency_sketch,
                    self._query_latency_sketch,
                )
                if self._verbose_logger is not None:
                    logging_fn = self._verbose_logger.info
                else:
                    logging_fn = logger.debug
                logging_fn(
                    "Sending metrics report: txn_completions_per_s: %.2f", sampled_thpt
                )
                self._output_queue.put_nowait(metrics_report)

                txn_p90 = self._txn_latency_sketch.get_quantile_value(0.9)
                if txn_p90 is not None:
                    logger.debug("Transaction latency p90 (s): %.4f", txn_p90)

                query_p90 = self._query_latency_sketch.get_quantile_value(0.9)
                if query_p90 is not None:
                    logger.debug("Query latency p90 (s): %.4f", query_p90)

                period_start = time.time()
                self._reset_latency_sketches()

        except Exception as ex:
            if not isinstance(ex, asyncio.CancelledError):
                # This should be a fatal error.
                logger.exception("Unexpected error in the metrics reporting task.")

    def _clean_query_str(self, raw_sql: str) -> str:
        sql = raw_sql.strip()
        if sql.endswith(";"):
            sql = sql[:-1]
        return sql.strip()

    async def _send_daemon_request(self, request: str) -> None:
        message = InternalCommandRequest(self._fe_index, request)
        logger.debug("Sending internal command request: %s", message)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._output_queue.put, message)

    async def _refresh_qlogger(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._config.epoch_length.total_seconds())
                await self._qhandler.refresh()
        finally:
            # Run one last refresh before exiting to ensure any remaining log
            # files are uploaded.
            await self._qhandler.refresh()

    async def _ping_watchdog(self) -> None:
        try:
            while True:
                await asyncio.sleep(60.0)  # TODO: Hardcoded
                self._watchdog.ping()
        except Exception as ex:
            if not isinstance(ex, asyncio.CancelledError):
                logger.exception("Watchdog ping task encountered exception.")

    async def _run_blueprint_update(
        self, version: int, updated_directory: Directory
    ) -> None:
        await self._blueprint_mgr.load(skip_directory_refresh=True)
        self._blueprint_mgr.get_directory().update_to_directory(updated_directory)
        active_version = self._blueprint_mgr.get_active_blueprint_version()
        if version != active_version:
            logger.error(
                "Retrieved active blueprint version (%d) is not the same as the notified version (%d).",
                active_version,
                version,
            )
            return

        blueprint = self._blueprint_mgr.get_blueprint()
        directory = self._blueprint_mgr.get_directory()
        logger.info("Loaded new directory: %s", directory)
        if self._monitor is not None:
            self._monitor.update_metrics_sources()
        await self._sessions.add_and_refresh_connections()
        assert self._router is not None
        self._router.update_blueprint(blueprint)
        # NOTE: This will cause any pending queries on the to-be-removed
        # connections to be cancelled. We consider this behavior to be
        # acceptable.
        await self._sessions.remove_connections()
        logger.info("Completed transition to blueprint version %d", version)

    def _schedule_reestablish_connections(self) -> None:
        if self._reestablish_connections_task is not None:
            return
        self._reestablish_connections_task = asyncio.create_task(
            self._do_reestablish_connections()
        )

    async def _do_reestablish_connections(self) -> None:
        try:
            # FIXME: This approach is not ideal because we introduce concurrent
            # access to the session manager.
            rand_backoff = None

            while True:
                if self._verbose_logger is not None:
                    self._verbose_logger.info(
                        "Attempting to re-establish lost connections."
                    )

                report = await self._sessions.reestablish_connections()

                if self._verbose_logger is not None:
                    self._verbose_logger.info("%s", str(report))

                if report.all_succeeded():
                    logger.debug("Re-established connections successfully.")
                    if self._verbose_logger is not None:
                        self._verbose_logger.debug(
                            "Re-established connections successfully."
                        )
                    self._reestablish_connections_task = None
                    break

                if rand_backoff is None:
                    rand_backoff = RandomizedExponentialBackoff(
                        max_retries=100,
                        base_delay_s=1.0,
                        max_delay_s=timedelta(minutes=1).total_seconds(),
                    )

                wait_time = rand_backoff.wait_time_s()
                if wait_time is None:
                    logger.warning(
                        "Abandoning connection re-establishment due to too many failures"
                    )
                    # N.B. We purposefully do not clear the
                    # `_reestablish_connections_task` variable.
                    break
                else:
                    await asyncio.sleep(wait_time)

                # N.B. We should not refresh the blueprint/directory here
                # because it can lead to AWS throttling. The directory only
                # changes during a blueprint transition; the daemon always
                # provides the latest directory to the front end on a
                # transition.

        except:  # pylint: disable=bare-except
            logger.exception("Unexpected failure when reestablishing connections.")
            self._reestablish_connections_task = None

    def _reset_latency_sketches(self) -> None:
        sketch_rel_accuracy = 0.01
        self._query_latency_sketch = DDSketch(relative_accuracy=sketch_rel_accuracy)
        self._txn_latency_sketch = DDSketch(relative_accuracy=sketch_rel_accuracy)


async def _orchestrate_shutdown() -> None:
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    loop = asyncio.get_event_loop()
    loop.stop()
