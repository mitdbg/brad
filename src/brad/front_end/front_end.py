import asyncio
import json
import logging
import random
import time
import multiprocessing as mp
from typing import AsyncIterable, Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone

import grpc
import pyodbc

import brad.proto_gen.brad_pb2_grpc as brad_grpc

from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.blueprint.diff.blueprint import BlueprintDiff
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.daemon.daemon import BradDaemon
from brad.daemon.monitor import Monitor
from brad.daemon.messages import ShutdownDaemon, NewBlueprint, Sentinel, MetricsReport
from brad.data_stats.estimator import Estimator
from brad.data_stats.postgres_estimator import PostgresEstimator
from brad.data_sync.execution.executor import DataSyncExecutor
from brad.query_rep import QueryRep
from brad.routing.always_one import AlwaysOneRouter
from brad.routing.rule_based import RuleBased
from brad.routing.location_aware_round_robin import LocationAwareRoundRobin
from brad.routing.policy import RoutingPolicy
from brad.routing.router import Router
from brad.routing.tree_based.forest_router import ForestRouter
from brad.front_end.brad_interface import BradInterface
from brad.front_end.blueprint_manager import BlueprintManager
from brad.front_end.epoch_file_handler import EpochFileHandler
from brad.front_end.errors import QueryError
from brad.front_end.grpc import BradGrpc
from brad.front_end.session import SessionManager, SessionId
from brad.utils.counter import Counter
from brad.utils.json_decimal_encoder import DecimalEncoder

logger = logging.getLogger(__name__)

LINESEP = "\n".encode()

RowList = List[Tuple[Any, ...]]


class BradFrontEnd(BradInterface):
    def __init__(
        self,
        config: ConfigFile,
        schema_name: str,
        path_to_planner_config: str,
        debug_mode: bool,
    ):
        self._config = config
        self._schema_name = schema_name
        self._debug_mode = debug_mode
        self._assets = AssetManager(self._config)
        self._blueprint_mgr = BlueprintManager(self._assets, self._schema_name)
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

        self._data_sync_executor = DataSyncExecutor(self._config, self._blueprint_mgr)
        self._timed_sync_task = None
        self._daemon_messages_task = None

        self._estimator: Optional[Estimator] = None

        # Used for managing the daemon process.
        self._daemon_mp_manager: Optional[mp.managers.SyncManager] = None
        self._daemon_input_queue: Optional[mp.Queue] = None
        self._daemon_output_queue: Optional[mp.Queue] = None
        self._daemon_process: Optional[mp.Process] = None

        # Number of transactions that completed.
        self._transaction_end_counter = Counter()
        self._brad_metrics_reporting_task = None

    async def serve_forever(self):
        await self.run_setup()
        try:
            grpc_server = grpc.aio.server()
            brad_grpc.add_BradServicer_to_server(BradGrpc(self), grpc_server)
            grpc_server.add_insecure_port(
                "{}:{}".format(self._config.server_interface, self._config.server_port)
            )
            await grpc_server.start()
            logger.info("The BRAD server has successfully started.")
            logger.info("Listening on port %d.", self._config.server_port)

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
            await self.run_teardown()
            logger.info("The BRAD server has shut down.")

    async def run_setup(self):
        await self._blueprint_mgr.load()
        logger.info("Using blueprint: %s", self._blueprint_mgr.get_blueprint())

        if self._config.data_sync_period_seconds > 0:
            self._timed_sync_task = asyncio.create_task(self._run_sync_periodically())
        await self._data_sync_executor.establish_connections()

        if self._routing_policy == RoutingPolicy.ForestTableSelectivity:
            self._estimator = await PostgresEstimator.connect(
                self._schema_name, self._config
            )
            await self._estimator.analyze(self._blueprint_mgr.get_blueprint())
        else:
            self._estimator = None
        await self._router.run_setup(self._estimator)

        # Launch the daemon process.
        self._daemon_mp_manager = mp.Manager()
        self._daemon_input_queue = self._daemon_mp_manager.Queue()
        self._daemon_output_queue = self._daemon_mp_manager.Queue()
        self._daemon_messages_task = asyncio.create_task(self._read_daemon_messages())
        self._daemon_process = mp.Process(
            target=BradDaemon.launch_in_subprocess,
            args=(
                self._config.raw_path,
                self._schema_name,
                self._blueprint_mgr.get_blueprint(),
                self._path_to_planner_config,
                self._debug_mode,
                self._daemon_input_queue,
                self._daemon_output_queue,
            ),
        )

        self._daemon_process.start()
        logger.info("The BRAD daemon process has been started.")

        # Start the metrics reporting task.
        self._brad_metrics_reporting_task = asyncio.create_task(
            self._report_metrics_to_daemon()
        )

    async def run_teardown(self):
        await self._sessions.end_all_sessions()

        loop = asyncio.get_event_loop()
        assert self._daemon_input_queue is not None
        assert self._daemon_output_queue is not None
        assert self._daemon_process is not None

        # Tell the daemon process to shut down and wait for it to do so.
        await loop.run_in_executor(None, self._daemon_input_queue.put, ShutdownDaemon())
        await loop.run_in_executor(None, self._daemon_process.join)

        # Important for unblocking our message reader thread.
        self._daemon_output_queue.put(Sentinel())

        if self._timed_sync_task is not None:
            await self._timed_sync_task.close()
            self._timed_sync_task = None

        await self._data_sync_executor.shutdown()

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

        if command == "BRAD_SYNC":
            logger.debug("Manually triggered a data sync.")
            ran_sync = await self._data_sync_executor.run_sync(
                self._blueprint_mgr.get_blueprint()
            )
            if ran_sync:
                return [("Sync succeeded.",)]
            else:
                return [("Sync skipped. No new writes to sync.",)]

        elif command == "BRAD_EXPLAIN_SYNC_STATIC":
            logical_plan = self._data_sync_executor.get_static_logical_plan(
                self._blueprint_mgr.get_blueprint()
            )
            to_return: RowList = [("Logical Data Sync Plan:",)]
            for str_op in logical_plan.traverse_plan_sequentially():
                to_return.append((str_op,))
            return to_return

        elif command == "BRAD_EXPLAIN_SYNC":
            logical, physical = await self._data_sync_executor.get_processed_plans(
                self._blueprint_mgr.get_blueprint()
            )
            to_return = [("Logical Data Sync Plan:",)]
            for str_op in logical.traverse_plan_sequentially():
                to_return.append((str_op,))
            to_return.append(("Physical Data Sync Plan:",))
            for str_op in physical.traverse_plan_sequentially():
                to_return.append((str_op,))
            return to_return

        else:
            return [("Unknown internal command: {}".format(command),)]

    async def _run_sync_periodically(self) -> None:
        while True:
            await asyncio.sleep(self._config.data_sync_period_seconds)
            logger.debug("Starting an auto data sync.")
            await self._data_sync_executor.run_sync(self._blueprint_mgr.get_blueprint())

    async def _read_daemon_messages(self) -> None:
        assert self._daemon_output_queue is not None
        loop = asyncio.get_running_loop()
        while True:
            message = await loop.run_in_executor(None, self._daemon_output_queue.get)

            if isinstance(message, NewBlueprint):
                await self._handle_new_blueprint(message.blueprint)

    async def _report_metrics_to_daemon(self) -> None:
        period_start = time.time()
        while True:
            txn_value = self._transaction_end_counter.value()
            period_end = time.time()
            self._transaction_end_counter.reset()
            elapsed_time_s = period_end - period_start

            # If the input queue is full, we just drop this message.
            sampled_thpt = txn_value / elapsed_time_s
            metrics_report = MetricsReport(sampled_thpt)
            logger.debug(
                "Sending metrics report: txn_completions_per_s: %.2f", sampled_thpt
            )
            assert self._daemon_input_queue is not None
            self._daemon_input_queue.put_nowait(metrics_report)

            period_start = time.time()

            # NOTE: Once we add multiple front end servers, we should stagger
            # the sleep period.
            await asyncio.sleep(self._config.front_end_metrics_reporting_period_seconds)

    async def _handle_new_blueprint(self, new_blueprint: Blueprint) -> None:
        # This is where we launch any reconfigurations needed to realize
        # the new blueprint.
        logger.debug("Received new blueprint: %s", new_blueprint)
        curr_blueprint = self._blueprint_mgr.get_blueprint()
        _diff = BlueprintDiff.of(curr_blueprint, new_blueprint)

        # - Provisioning changes handled here (if there are blueprint changes).
        # - Need to update the blueprint stored in the manager.

    def _clean_query_str(self, raw_sql: str) -> str:
        sql = raw_sql.strip()
        if sql.endswith(";"):
            sql = sql[:-1]
        return sql.strip()
