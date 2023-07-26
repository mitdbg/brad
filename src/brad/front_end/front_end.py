import asyncio
import json
import logging
import random
import time
import multiprocessing as mp
from typing import AsyncIterable, Optional, Dict, Any
from datetime import datetime, timezone

import grpc
import pyodbc

import brad.proto_gen.brad_pb2_grpc as brad_grpc

from brad.asset_manager import AssetManager
from brad.blueprint_manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.daemon.monitor import Monitor
from brad.daemon.messages import (
    ShutdownFrontEnd,
    Sentinel,
    MetricsReport,
    InternalCommandRequest,
    InternalCommandResponse,
)
from brad.data_stats.estimator import Estimator
from brad.data_stats.postgres_estimator import PostgresEstimator
from brad.front_end.brad_interface import BradInterface
from brad.front_end.epoch_file_handler import EpochFileHandler
from brad.front_end.errors import QueryError
from brad.front_end.grpc import BradGrpc
from brad.front_end.session import SessionManager, SessionId
from brad.query_rep import QueryRep
from brad.routing.always_one import AlwaysOneRouter
from brad.routing.rule_based import RuleBased
from brad.routing.location_aware_round_robin import LocationAwareRoundRobin
from brad.routing.policy import RoutingPolicy
from brad.routing.router import Router
from brad.routing.tree_based.forest_router import ForestRouter
from brad.row_list import RowList
from brad.utils.counter import Counter
from brad.utils.json_decimal_encoder import DecimalEncoder
from brad.utils.mailbox import Mailbox

logger = logging.getLogger(__name__)

LINESEP = "\n".encode()


class BradFrontEnd(BradInterface):
    def __init__(
        self,
        fe_index: int,
        config: ConfigFile,
        schema_name: str,
        path_to_planner_config: str,
        debug_mode: bool,
        input_queue: mp.Queue,
        output_queue: mp.Queue,
    ):
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
            self._config, self._assets, self._schema_name
        )
        self._path_to_planner_config = path_to_planner_config
        self._monitor: Optional[Monitor] = None

        # Set up query logger
        self._qlogger = logging.getLogger("queries")
        self._qlogger.setLevel(logging.INFO)
        qhandler = EpochFileHandler(
            self._config.local_logs_path,
            self._config.epoch_length,
            self._config.s3_logs_bucket,
            self._config.s3_logs_path,
            self._config.txn_log_prob,
        )
        qhandler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(message)s")
        qhandler.setFormatter(formatter)
        self._qlogger.addHandler(qhandler)
        self._qlogger.propagate = False  # Avoids printing to stdout

        # We have different routing policies for performance evaluation and
        # testing purposes.
        routing_policy = self._config.routing_policy
        self._routing_policy = routing_policy
        if routing_policy == RoutingPolicy.Default:
            self._router: Router = LocationAwareRoundRobin(self._blueprint_mgr)
        elif routing_policy == RoutingPolicy.AlwaysAthena:
            self._router = AlwaysOneRouter(Engine.Athena)
        elif routing_policy == RoutingPolicy.AlwaysAurora:
            self._router = AlwaysOneRouter(Engine.Aurora)
        elif routing_policy == RoutingPolicy.AlwaysRedshift:
            self._router = AlwaysOneRouter(Engine.Redshift)
        elif routing_policy == RoutingPolicy.RuleBased:
            self._monitor = Monitor.from_config_file(config)
            self._router = RuleBased(
                blueprint_mgr=self._blueprint_mgr, monitor=self._monitor
            )
        elif (
            routing_policy == RoutingPolicy.ForestTablePresence
            or routing_policy == RoutingPolicy.ForestTableSelectivity
        ):
            self._router = ForestRouter.for_server(
                routing_policy, self._schema_name, self._assets, self._blueprint_mgr
            )
        else:
            raise RuntimeError(
                "Unsupported routing policy: {}".format(str(routing_policy))
            )
        logger.info("Using routing policy: %s", routing_policy)

        self._sessions = SessionManager(self._config, self._schema_name)
        self._daemon_messages_task: Optional[asyncio.Task[None]] = None
        self._estimator: Optional[Estimator] = None

        # Number of transactions that completed.
        self._transaction_end_counter = Counter()
        self._brad_metrics_reporting_task: Optional[asyncio.Task[None]] = None

        # Used to manage `BRAD_` requests that need to be sent to the daemon for
        # execution. We only allow one such request to be in flight at any time
        # (to simplify the logic here, since we only need it for completeness).
        self._daemon_request_mailbox = Mailbox[str, RowList](
            do_send_msg=self._send_daemon_request
        )

    async def serve_forever(self):
        await self._run_setup()
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

            if self._routing_policy == RoutingPolicy.RuleBased:
                assert (
                    self._monitor is not None
                ), "require monitor running for rule-based router"
                await asyncio.gather(
                    self._monitor.run_forever(), grpc_server.wait_for_termination()
                )
            else:
                await grpc_server.wait_for_termination()
        finally:
            # Not ideal, but we need to manually call this method to ensure
            # gRPC's internal shutdown process completes before we return from
            # this method.
            grpc_server.__del__()
            await self._run_teardown()
            logger.debug("BRAD front end _run_teardown() complete.")

    async def _run_setup(self) -> None:
        await self._blueprint_mgr.load()
        logger.info("Using blueprint: %s", self._blueprint_mgr.get_blueprint())

        if self._routing_policy == RoutingPolicy.ForestTableSelectivity:
            self._estimator = await PostgresEstimator.connect(
                self._schema_name, self._config
            )
            await self._estimator.analyze(self._blueprint_mgr.get_blueprint())
        else:
            self._estimator = None
        await self._router.run_setup(self._estimator)

        # Start the metrics reporting task.
        self._brad_metrics_reporting_task = asyncio.create_task(
            self._report_metrics_to_daemon()
        )

        # Used to handle messages from the daemon.
        self._daemon_messages_task = asyncio.create_task(self._read_daemon_messages())

    async def _run_teardown(self):
        logger.debug("Starting BRAD front end _run_teardown()")
        await self._sessions.end_all_sessions()

        # Important for unblocking our message reader thread.
        self._input_queue.put(Sentinel())

        if self._daemon_messages_task is not None:
            self._daemon_messages_task.cancel()
            self._daemon_messages_task = None

        if self._brad_metrics_reporting_task is not None:
            self._brad_metrics_reporting_task.cancel()
            self._brad_metrics_reporting_task = None

        if self._estimator is not None:
            await self._estimator.close()
            self._estimator = None

    async def start_session(self) -> SessionId:
        session_id, _ = await self._sessions.create_new_session()
        return session_id

    async def end_session(self, session_id: SessionId) -> None:
        await self._sessions.end_session(session_id)

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
            raise QueryError("Invalid session id {}".format(str(session_id)))

        try:
            # Remove any trailing or leading whitespace. Remove the trailing
            # semicolon if it exists.
            # NOTE: BRAD does not yet support having multiple
            # semicolon-separated queries in one request.
            query = self._clean_query_str(query)

            # Handle internal commands separately.
            if query.startswith("BRAD_"):
                return await self._handle_internal_command(query)

            # 2. Select an engine for the query.
            query_rep = QueryRep(query)
            transactional_query = (
                session.in_transaction or query_rep.is_data_modification_query()
            )
            if transactional_query:
                engine_to_use = Engine.Aurora
            else:
                engine_to_use = await self._router.engine_for(query_rep)

            logger.debug(
                "[S%d] Routing '%s' to %s", session_id.value(), query, engine_to_use
            )
            debug_info["executor"] = engine_to_use

            # 3. Actually execute the query.
            connection = session.engines.get_connection(engine_to_use)
            cursor = await connection.cursor()
            try:
                start = datetime.now(tz=timezone.utc)
                await cursor.execute(query_rep.raw_query)
                end = datetime.now(tz=timezone.utc)
            except (pyodbc.ProgrammingError, pyodbc.Error) as ex:
                # Error when executing the query.
                raise QueryError.from_exception(ex)

            # We keep track of transactional state after executing the query in
            # case the query failed.
            if query_rep.is_transaction_start():
                session.set_in_transaction(in_txn=True)

            if query_rep.is_transaction_end():
                session.set_in_transaction(in_txn=False)
                self._transaction_end_counter.bump()

            # Decide whether to log the query.
            if not transactional_query or (random.random() < self._config.txn_log_prob):
                self._qlogger.info(
                    f"{end.strftime('%Y-%m-%d %H:%M:%S,%f')} INFO Query: {query} Engine: {engine_to_use} Duration: {end-start}s IsTransaction: {transactional_query}"
                )

            # Extract and return the results, if any.
            try:
                results = []
                while True:
                    row = await cursor.fetchone()
                    if row is None:
                        break
                    results.append(tuple(row))
                logger.debug("Responded with %d rows.", len(results))
                return results
            except pyodbc.ProgrammingError:
                logger.debug("No rows produced.")
                return []

        except QueryError:
            # This is an expected exception. We catch and re-raise it here to
            # avoid triggering the handler below.
            raise
        except Exception as ex:
            logger.exception("Encountered unexpected exception when handling request.")
            raise QueryError.from_exception(ex)

    async def _handle_internal_command(self, command_raw: str) -> RowList:
        """
        This method is used to handle BRAD_ prefixed "queries" (i.e., commands
        to run custom functionality like syncing data across the engines).
        """
        command = command_raw.upper()

        if (
            command == "BRAD_SYNC"
            or command == "BRAD_EXPLAIN_SYNC_STATIC"
            or command == "BRAD_EXPLAIN_SYNC"
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

        else:
            return [("Unknown internal command: {}".format(command),)]

    async def _read_daemon_messages(self) -> None:
        assert self._input_queue is not None
        loop = asyncio.get_running_loop()
        while True:
            message = await loop.run_in_executor(None, self._input_queue.get)

            if isinstance(message, ShutdownFrontEnd):
                logger.debug("The BRAD front end is initiating a shut down...")
                loop.create_task(_orchestrate_shutdown())
                break
            elif isinstance(message, InternalCommandResponse):
                if message.fe_index != self._fe_index:
                    logger.warning(
                        "Received message with invalid front end index. Expected %d. Received %d.",
                        self._fe_index,
                        message.fe_index,
                    )
                    continue
                if not self._daemon_request_mailbox.is_active():
                    logger.warning(
                        "Received an internal command response but no one was waiting for it: %s",
                        message,
                    )
                    continue
                self._daemon_request_mailbox.on_new_message(message.response)
            else:
                logger.info("Received message from the daemon: %s", message)

    async def _report_metrics_to_daemon(self) -> None:
        period_start = time.time()
        while True:
            txn_value = self._transaction_end_counter.value()
            period_end = time.time()
            self._transaction_end_counter.reset()
            elapsed_time_s = period_end - period_start

            # If the input queue is full, we just drop this message.
            sampled_thpt = txn_value / elapsed_time_s
            metrics_report = MetricsReport(self._fe_index, sampled_thpt)
            logger.debug(
                "Sending metrics report: txn_completions_per_s: %.2f", sampled_thpt
            )
            self._output_queue.put_nowait(metrics_report)

            period_start = time.time()

            # NOTE: Once we add multiple front end servers, we should stagger
            # the sleep period.
            await asyncio.sleep(self._config.front_end_metrics_reporting_period_seconds)

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


async def _orchestrate_shutdown() -> None:
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    loop = asyncio.get_event_loop()
    loop.stop()
