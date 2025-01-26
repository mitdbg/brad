import asyncio
import logging
import ssl
import multiprocessing as mp
import redshift_connector.error as redshift_errors
import psycopg
import struct
from typing import Optional, Dict, Any, Tuple
from datetime import timedelta
from ddsketch import DDSketch
import pyodbc

from brad.asset_manager import AssetManager
from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.connection.connection import ConnectionFailed
from brad.connection.schema import Schema
from brad.daemon.monitor import Monitor
from brad.daemon.messages import (
    ShutdownFrontEnd,
    Sentinel,
    VdbeMetricsReport,
    NewBlueprint,
    NewBlueprintAck,
)
from brad.front_end.errors import QueryError
from brad.front_end.session import SessionManager, SessionId
from brad.front_end.watchdog import Watchdog
from brad.provisioning.directory import Directory
from brad.row_list import RowList
from brad.utils import log_verbose, create_custom_logger
from brad.utils.rand_exponential_backoff import RandomizedExponentialBackoff
from brad.utils.run_time_reservoir import RunTimeReservoir
from brad.utils.time_periods import universal_now
from brad.vdbe.manager import VdbeFrontEndManager
from brad.vdbe.models import VirtualInfrastructure
from brad.front_end.vdbe.vdbe_endpoint_manager import VdbeEndpointManager

logger = logging.getLogger(__name__)

LINESEP = "\n".encode()


class BradVdbeFrontEnd:
    NUMERIC_IDENTIFIER = 10101

    def __init__(
        self,
        config: ConfigFile,
        schema_name: str,
        path_to_system_config: str,
        debug_mode: bool,
        initial_directory: Directory,
        initial_infra: VirtualInfrastructure,
        input_queue: mp.Queue,
        output_queue: mp.Queue,
    ):
        self._main_thread_loop: Optional[asyncio.AbstractEventLoop] = None

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

        # Used to track query performance.
        self._query_run_times = RunTimeReservoir[float](
            self._config.front_end_query_latency_buffer_size
        )

        self._sessions = SessionManager(
            self._config, self._blueprint_mgr, self._schema_name, for_vdbes=True
        )
        self._daemon_messages_task: Optional[asyncio.Task[None]] = None

        self._reset_latency_sketches()
        self._brad_metrics_reporting_task: Optional[asyncio.Task[None]] = None

        # Used to re-establish engine connections.
        self._reestablish_connections_task: Optional[asyncio.Task[None]] = None

        # Used for logging transient errors that are too verbose.
        main_log_file = config.front_end_log_file(self.NUMERIC_IDENTIFIER)
        if main_log_file is not None:
            verbose_log_file = (
                main_log_file.parent
                / f"brad_front_end_verbose_{self.NUMERIC_IDENTIFIER}.log"
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

        self._is_stub_mode = self._config.stub_mode_path() is not None

        self._vdbe_mgr = VdbeFrontEndManager(initial_infra)
        self._endpoint_mgr = VdbeEndpointManager(
            vdbe_mgr=self._vdbe_mgr,
            session_mgr=self._sessions,
            handler=self._run_query_impl,
        )

    async def serve_forever(self):
        await self._run_setup()
        try:
            # Wait forever. The server is shut down when we receive a shutdown
            # message and this task gets cancelled externally.
            event = asyncio.Event()
            await event.wait()
        finally:
            await self._run_teardown()
            logger.debug("BRAD VDBE front end _run_teardown() complete.")

    async def _run_setup(self) -> None:
        self._main_thread_loop = asyncio.get_running_loop()

        # The directory will have been populated by the daemon.
        await self._blueprint_mgr.load(skip_directory_refresh=True)
        logger.info("Using blueprint: %s", self._blueprint_mgr.get_blueprint())

        if self._monitor is not None:
            self._monitor.set_up_metrics_sources()
            await self._monitor.fetch_latest()

        # Start the metrics reporting task.
        self._brad_metrics_reporting_task = asyncio.create_task(
            self._report_metrics_to_daemon()
        )

        # Used to handle messages from the daemon.
        self._daemon_messages_task = asyncio.create_task(self._read_daemon_messages())

        self._watchdog.start(self._main_thread_loop)
        self._ping_watchdog_task = asyncio.create_task(self._ping_watchdog())

        # Start all VDBE endpoints.
        await self._endpoint_mgr.initialize()

    async def _run_teardown(self):
        logger.debug("Starting BRAD VDBE front end _run_teardown()")

        # Stop all VDBE endpoints (this will also end the sessions).
        await self._endpoint_mgr.shutdown()

        # Important for unblocking our message reader thread.
        self._input_queue.put(Sentinel(self.NUMERIC_IDENTIFIER))

        if self._daemon_messages_task is not None:
            self._daemon_messages_task.cancel()
            self._daemon_messages_task = None

        if self._brad_metrics_reporting_task is not None:
            self._brad_metrics_reporting_task.cancel()
            self._brad_metrics_reporting_task = None

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

    async def _run_query_impl(
        self,
        query: str,
        vdbe_id: int,
        session_id: SessionId,
        debug_info: Dict[str, Any],
        retrieve_schema: bool = False,
    ) -> Tuple[RowList, Optional[Schema]]:
        session = self._sessions.get_session(session_id)
        if session is None:
            raise QueryError(
                "Invalid session id {}".format(str(session_id)), is_transient=False
            )

        vdbe = self._vdbe_mgr.engine_by_id(vdbe_id)
        if vdbe is None:
            raise QueryError(
                "Invalid VDBE id {}".format(str(vdbe_id)), is_transient=False
            )

        try:
            # Remove any trailing or leading whitespace. Remove the trailing
            # semicolon if it exists.
            # NOTE: BRAD does not yet support having multiple
            # semicolon-separated queries in one request.
            query = self._clean_query_str(query)

            # TODO: Validate table accesses.
            engine_to_use = vdbe.mapped_to

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
                connection = session.engines.get_reader_connection(engine_to_use)
                cursor = connection.cursor_sync()
                # HACK: To work around dialect differences between
                # Athena/Aurora/Redshift for now. This should be replaced by
                # a more robust translation layer.
                if engine_to_use == Engine.Athena and "ascii" in query:
                    translated_query = query.replace("ascii", "codepoint")
                else:
                    translated_query = query
                start = universal_now()
                await cursor.execute(translated_query)
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

            # Decide whether to log the query.
            run_time_s = end - start
            run_time_s_float = run_time_s.total_seconds()
            # TODO: Should be per VDBE.
            self._query_latency_sketch.add(run_time_s_float)

            # Extract and return the results, if any.
            try:
                result_row_limit = self._config.result_row_limit()
                if result_row_limit is not None:
                    results = []
                    for _ in range(result_row_limit):
                        row = cursor.fetchone_sync()
                        if row is None:
                            break
                        results.append(tuple(row))
                    log_verbose(
                        logger,
                        "Responded with %d rows (limited to %d rows).",
                        len(results),
                    )
                else:
                    # Using `fetchall_sync()` is lower overhead than the async interface.
                    results = [tuple(row) for row in cursor.fetchall_sync()]
                    log_verbose(logger, "Responded with %d rows.", len(results))
                return (
                    results,
                    (cursor.result_schema(results) if retrieve_schema else None),
                )
            except (pyodbc.ProgrammingError, psycopg.ProgrammingError):
                log_verbose(logger, "No rows produced.")
                return ([], Schema.empty() if retrieve_schema else None)
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

    async def _read_daemon_messages(self) -> None:
        assert self._input_queue is not None
        loop = asyncio.get_running_loop()
        while True:
            try:
                message = await loop.run_in_executor(None, self._input_queue.get)
                if message.fe_index != self.NUMERIC_IDENTIFIER:
                    logger.warning(
                        "Received message with invalid front end index. Expected %d. Received %d.",
                        self.NUMERIC_IDENTIFIER,
                        message.fe_index,
                    )
                    continue

                if isinstance(message, ShutdownFrontEnd):
                    logger.debug("The BRAD front end is initiating a shut down...")
                    loop.create_task(_orchestrate_shutdown())
                    break

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
                        NewBlueprintAck(self.NUMERIC_IDENTIFIER, message.version),
                        block=False,
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
            # We want to stagger the reports across the front ends to avoid
            # overwhelming the daemon.
            await asyncio.sleep(0.1 * 10)

            while True:
                # Ideally we adjust for delays here too.
                await asyncio.sleep(
                    self._config.front_end_metrics_reporting_period_seconds
                )

                # If the input queue is full, we just drop this message.
                metrics_report = VdbeMetricsReport.from_data(
                    self.NUMERIC_IDENTIFIER,
                    [(0, self._query_latency_sketch)],
                )
                self._output_queue.put_nowait(metrics_report)

                query_p90 = self._query_latency_sketch.get_quantile_value(0.9)
                if query_p90 is not None:
                    logger.debug("Query latency p90 (s): %.4f", query_p90)

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

        directory = self._blueprint_mgr.get_directory()
        logger.info("Loaded new directory: %s", directory)
        if self._monitor is not None:
            self._monitor.update_metrics_sources()
        await self._sessions.add_and_refresh_connections()
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
        # TODO: Store per VDBE.
        sketch_rel_accuracy = 0.01
        self._query_latency_sketch = DDSketch(relative_accuracy=sketch_rel_accuracy)


async def _orchestrate_shutdown() -> None:
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    loop = asyncio.get_event_loop()
    loop.stop()
