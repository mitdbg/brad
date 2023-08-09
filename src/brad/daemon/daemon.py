import asyncio
import logging
import queue
import pytz
import os
import multiprocessing as mp
from typing import Optional, List
from datetime import datetime

from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.blueprint.manager import BlueprintManager
from brad.blueprint.state import TransitionState
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.config.temp_config import TempConfig
from brad.daemon.messages import (
    ShutdownFrontEnd,
    Sentinel,
    MetricsReport,
    InternalCommandRequest,
    InternalCommandResponse,
    NewBlueprint,
    NewBlueprintAck,
)
from brad.daemon.monitor import Monitor
from brad.daemon.transition_orchestrator import TransitionOrchestrator
from brad.data_stats.estimator import Estimator
from brad.data_stats.postgres_estimator import PostgresEstimator
from brad.data_sync.execution.executor import DataSyncExecutor
from brad.front_end.start_front_end import start_front_end
from brad.planner.abstract import BlueprintPlanner
from brad.planner.compare.cost import best_cost_under_p99_latency
from brad.planner.estimator import EstimatorProvider
from brad.planner.factory import BlueprintPlannerFactory
from brad.planner.metrics import MetricsFromMonitor
from brad.planner.scoring.data_access.provider import DataAccessProvider
from brad.planner.scoring.data_access.precomputed_values import (
    PrecomputedDataAccessProvider,
)
from brad.planner.scoring.performance.analytics_latency import AnalyticsLatencyScorer
from brad.planner.scoring.performance.precomputed_predictions import (
    PrecomputedPredictions,
)
from brad.planner.workload import Workload
from brad.planner.workload.builder import WorkloadBuilder
from brad.planner.workload.provider import LoggedWorkloadProvider
from brad.routing.policy import RoutingPolicy
from brad.row_list import RowList
from brad.utils.time_periods import period_start

logger = logging.getLogger(__name__)

# Temporarily used.
PERSIST_BLUEPRINT_VAR = "BRAD_PERSIST_BLUEPRINT"


class BradDaemon:
    """
    Represents BRAD's controller process.

    This code is written with the assumption that this daemon is spawned by the
    BRAD server. In the future, we may want the daemon to be launched
    independently and for it to communicate with the server via RPCs.
    """

    def __init__(
        self,
        config: ConfigFile,
        temp_config: Optional[TempConfig],
        schema_name: str,
        path_to_planner_config: str,
        debug_mode: bool,
    ):
        self._config = config
        self._temp_config = temp_config
        self._schema_name = schema_name
        self._path_to_planner_config = path_to_planner_config
        self._planner_config = PlannerConfig(path_to_planner_config)
        self._debug_mode = debug_mode

        self._assets = AssetManager(self._config)
        self._blueprint_mgr = BlueprintManager(
            self._config, self._assets, self._schema_name
        )
        self._monitor = Monitor(self._config, self._blueprint_mgr)
        self._estimator_provider = _EstimatorProvider()
        self._planner: Optional[BlueprintPlanner] = None

        self._process_manager: Optional[mp.managers.SyncManager] = None
        self._front_ends: List[_FrontEndProcess] = []

        self._data_sync_executor = DataSyncExecutor(self._config, self._blueprint_mgr)
        self._timed_sync_task: Optional[asyncio.Task[None]] = None

        self._transition_orchestrator: Optional[TransitionOrchestrator] = None
        self._transition_task: Optional[asyncio.Task[None]] = None

    async def run_forever(self) -> None:
        """
        Starts running the daemon.
        """
        try:
            logger.info("Starting up the BRAD daemon...")
            await self._run_setup()
            assert self._planner is not None
            message_reader_tasks = [
                fe.message_reader_task
                for fe in self._front_ends
                if fe.message_reader_task is not None
            ]
            logger.info("The BRAD daemon is running.")
            await asyncio.gather(
                self._planner.run_forever(),
                self._monitor.run_forever(),
                *message_reader_tasks,
            )
        except Exception:
            logger.exception("The BRAD daemon encountered an unexpected exception.")
            raise
        finally:
            logger.info("The BRAD daemon is shutting down...")
            await self._run_teardown()
            logger.info("The BRAD daemon has shut down.")

    async def _run_setup(self) -> None:
        await self._blueprint_mgr.load()
        logger.info("Current blueprint: %s", self._blueprint_mgr.get_blueprint())

        # Initialize the monitor.
        self._monitor.set_up_metrics_sources()

        if self._config.data_sync_period_seconds > 0:
            self._timed_sync_task = asyncio.create_task(self._run_sync_periodically())
        await self._data_sync_executor.establish_connections()

        if self._temp_config is not None:
            # TODO: Actually call into the models. We avoid doing so for now to
            # avoid having to implement model loading, etc.
            latency_scorer: AnalyticsLatencyScorer = PrecomputedPredictions.load(
                workload_file_path=self._temp_config.query_bank_path(),
                aurora_predictions_path=self._temp_config.aurora_preds_path(),
                redshift_predictions_path=self._temp_config.redshift_preds_path(),
                athena_predictions_path=self._temp_config.athena_preds_path(),
            )
            data_access_provider = PrecomputedDataAccessProvider.load(
                workload_file_path=self._temp_config.query_bank_path(),
                aurora_accessed_pages_path=self._temp_config.aurora_data_access_path(),
                athena_accessed_bytes_path=self._temp_config.athena_data_access_path(),
            )
            comparator = best_cost_under_p99_latency(
                max_latency_ceiling_s=self._temp_config.latency_ceiling_s()
            )
        else:
            logger.warning(
                "TempConfig not provided. The planner will not be able to run correctly."
            )
            latency_scorer = _NoopAnalyticsScorer()
            data_access_provider = _NoopDataAccessProvider()
            comparator = best_cost_under_p99_latency(max_latency_ceiling_s=10)

        self._planner = BlueprintPlannerFactory.create(
            planner_config=self._planner_config,
            current_blueprint=self._blueprint_mgr.get_blueprint(),
            monitor=self._monitor,
            config=self._config,
            schema_name=self._schema_name,
            workload_provider=LoggedWorkloadProvider(
                self._config,
                self._planner_config,
                self._blueprint_mgr,
                self._schema_name,
            ),
            analytics_latency_scorer=latency_scorer,
            comparator=comparator,
            metrics_provider=MetricsFromMonitor(self._monitor),
            data_access_provider=data_access_provider,
            estimator_provider=self._estimator_provider,
        )
        self._planner.register_new_blueprint_callback(self._handle_new_blueprint)

        # Create and start the front end processes.
        logger.info(
            "Setting up and starting %d front ends...", self._config.num_front_ends
        )
        self._process_manager = mp.Manager()
        for fe_index in range(self._config.num_front_ends):
            input_queue = self._process_manager.Queue()
            output_queue = self._process_manager.Queue()
            process = mp.Process(
                target=start_front_end,
                args=(
                    fe_index,
                    self._config.raw_path,
                    self._schema_name,
                    self._path_to_planner_config,
                    self._debug_mode,
                    input_queue,
                    output_queue,
                ),
            )
            wrapper = _FrontEndProcess(fe_index, process, input_queue, output_queue)
            reader_task = asyncio.create_task(self._read_front_end_messages(wrapper))
            wrapper.message_reader_task = reader_task
            self._front_ends.append(wrapper)

        for fe in self._front_ends:
            fe.process.start()

        if self._config.routing_policy == RoutingPolicy.ForestTableSelectivity:
            logger.info("Setting up the cardinality estimator...")
            estimator = await PostgresEstimator.connect(self._schema_name, self._config)
            await estimator.analyze(self._blueprint_mgr.get_blueprint())
            self._estimator_provider.set_estimator(estimator)

    async def _run_teardown(self) -> None:
        # Shut down the front end processes.
        # 1. Send a message to tell them to shut down.
        # 2. Input sentinel messages to unblock our reader tasks.
        # 3. Wait for the processes to shut down.
        logger.info("Telling %d front end(s) to shut down...", len(self._front_ends))
        for fe_index, fe in enumerate(self._front_ends):
            fe.input_queue.put(ShutdownFrontEnd(fe_index))
            if fe.message_reader_task is not None:
                fe.output_queue.put(Sentinel(fe_index))

        if self._timed_sync_task is not None:
            self._timed_sync_task.cancel()
            self._timed_sync_task = None

        await self._data_sync_executor.shutdown()

        # Shut down the estimator.
        estimator = self._estimator_provider.get_estimator()
        if estimator is not None:
            await estimator.close()

        # Now wait for the front end processes to shut down.
        logger.info(
            "Waiting for %d front end(s) to shut down...", len(self._front_ends)
        )
        for fe in self._front_ends:
            fe.process.join()
        self._front_ends.clear()

    async def _read_front_end_messages(self, front_end: "_FrontEndProcess") -> None:
        """
        Waits for messages from the specified front end process and processes them.
        """
        loop = asyncio.get_running_loop()
        while True:
            message = await loop.run_in_executor(None, front_end.output_queue.get)
            if message.fe_index != front_end.fe_index:
                logger.warning(
                    "Received message with invalid front end index. Expected %d. Received %d.",
                    front_end.fe_index,
                    message.fe_index,
                )
                continue

            if isinstance(message, MetricsReport):
                self._monitor.handle_metric_report(message)

            elif isinstance(message, InternalCommandRequest):
                asyncio.create_task(
                    self._run_internal_command_request_response(message)
                )

            elif isinstance(message, NewBlueprintAck):
                if self._transition_orchestrator is None:
                    logger.error(
                        "Received blueprint ack message but no transition is in progress. Version: %d, Front end: %d",
                        message.version,
                        message.fe_index,
                    )
                    continue

                # Sanity check.
                next_version = self._transition_orchestrator.next_version()
                if next_version != message.version:
                    logger.error(
                        "Received a blueprint ack for a mismatched version. Received %d, Expected %d",
                        message.version,
                        next_version,
                    )
                    continue

                self._transition_orchestrator.decrement_waiting_for_front_ends()
                if self._transition_orchestrator.waiting_for_front_ends() == 0:
                    # Schedule the second half of the transition.
                    self._transition_task = asyncio.create_task(
                        self._run_transition_part_two()
                    )

            else:
                logger.debug(
                    "Received unexpected message from front end %d: %s",
                    front_end.fe_index,
                    str(message),
                )

    async def _handle_new_blueprint(self, blueprint: Blueprint) -> None:
        """
        Informs the server about a new blueprint.
        """
        if self._transition_orchestrator is not None:
            logger.warning(
                "Planner selected a new blueprint, but we are still transitioning to a blueprint. New blueprint: %s",
                blueprint,
            )
            return

        if PERSIST_BLUEPRINT_VAR in os.environ:
            logger.info(
                "Force-persisting the new blueprint. Restart BRAD to load the new blueprint."
            )
            await self._blueprint_mgr.start_transition(blueprint)
            await self._blueprint_mgr.update_transition_state(TransitionState.Stable)
            assert self._planner is not None
            self._planner.update_blueprint(blueprint)
        else:
            logger.info(
                "Planner selected a new blueprint. Transition is starting. New blueprint: %s",
                blueprint,
            )
            logger.info("Ignoring the blueprint (temporarily) for stability.")
            return

            # pylint: disable-next=unreachable
            await self._blueprint_mgr.start_transition(blueprint)
            self._transition_orchestrator = TransitionOrchestrator(
                self._config, self._blueprint_mgr
            )
            self._transition_task = asyncio.create_task(self._run_transition_part_one())

    async def _run_sync_periodically(self) -> None:
        while True:
            await asyncio.sleep(self._config.data_sync_period_seconds)
            logger.debug("Starting an auto data sync.")
            await self._data_sync_executor.run_sync(self._blueprint_mgr.get_blueprint())

    async def _run_internal_command_request_response(
        self, msg: InternalCommandRequest
    ) -> None:
        results = await self._handle_internal_command(msg.request)
        response = InternalCommandResponse(msg.fe_index, results)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, self._front_ends[msg.fe_index].input_queue.put, response
        )

    async def _handle_internal_command(self, command: str) -> RowList:
        if command == "BRAD_SYNC":
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

        elif command.startswith("BRAD_INSPECT_WORKLOAD"):
            now = datetime.now().astimezone(pytz.utc)
            epoch_length = self._config.epoch_length
            planning_window = self._planner_config.planning_window()
            window_end = period_start(now, self._config.epoch_length) + epoch_length

            parts = command.split(" ")
            if len(parts) > 1:
                try:
                    window_multiplier = int(parts[-1])
                except ValueError:
                    window_multiplier = 1
            else:
                window_multiplier = 1

            window_start = window_end - planning_window * window_multiplier
            w = (
                WorkloadBuilder()
                .add_queries_from_s3_logs(self._config, window_start, window_end)
                .build()
            )

            return [
                ("Unique AP queries", len(w.analytical_queries())),
                ("Unique TP queries", len(w.transactional_queries())),
                ("Period", w.period()),
                ("Window start (UTC)", str(window_start)),
                ("Window end (UTC)", str(window_end)),
            ]

        elif command.startswith("BRAD_RUN_PLANNER"):
            if self._planner is None:
                return [("Planner not yet initialized.",)]

            parts = command.split(" ")
            if len(parts) > 1:
                try:
                    window_multiplier = int(parts[-1])
                except ValueError:
                    window_multiplier = 1
            else:
                window_multiplier = 1

            logger.info("Triggering the planner based on an external request...")
            try:
                await self._planner.run_replan(window_multiplier)
                return [("Planner completed. See the daemon's logs for more details.",)]
            except Exception as ex:
                logger.exception("Encountered exception when running the planner.")
                return [(str(ex),)]

        else:
            logger.warning("Received unknown internal command: %s", command)
            return []

    async def _run_transition_part_one(self) -> None:
        assert self._transition_orchestrator is not None
        await self._transition_orchestrator.run_prepare_then_transition()

        # Switch to the transitioned state.
        tm = self._blueprint_mgr.get_transition_metadata()
        assert (
            tm.state == TransitionState.TransitionedPreCleanUp
        ), "Incorrect transition state."
        assert tm.next_version is not None, "Missing next version."

        # Important because the instance IDs may have changed.
        self._monitor.update_metrics_sources()

        await self._data_sync_executor.update_connections()

        # Inform all front ends about the new blueprint.
        logger.debug(
            "Notifying %d front ends about the new blueprint.", len(self._front_ends)
        )
        self._transition_orchestrator.set_waiting_for_front_ends(len(self._front_ends))
        for fe in self._front_ends:
            fe.input_queue.put(NewBlueprint(fe.fe_index, tm.next_version))

        self._transition_task = None

        # We finish the transition after all front ends acknowledge that they
        # have transitioned to the new blueprint.

    async def _run_transition_part_two(self) -> None:
        assert self._transition_orchestrator is not None
        await self._transition_orchestrator.run_clean_up_after_transition()
        if self._planner is not None:
            self._planner.update_blueprint(self._blueprint_mgr.get_blueprint())

        # Done.
        tm = self._blueprint_mgr.get_transition_metadata()
        assert (
            tm.state == TransitionState.Stable
        ), "Incorrect transition state after completion."
        logger.info("Completed the transition to blueprint version %d", tm.curr_version)
        self._transition_task = None
        self._transition_orchestrator = None


class _NoopAnalyticsScorer(AnalyticsLatencyScorer):
    def apply_predicted_latencies(self, _workload: Workload) -> None:
        pass


class _NoopDataAccessProvider(DataAccessProvider):
    def apply_access_statistics(self, _workload: Workload) -> None:
        pass


class _EstimatorProvider(EstimatorProvider):
    def __init__(self) -> None:
        self._estimator: Optional[Estimator] = None

    def set_estimator(self, estimator: Estimator) -> None:
        self._estimator = estimator

    def get_estimator(self) -> Optional[Estimator]:
        return self._estimator


class _FrontEndProcess:
    """
    Used to manage state associated with each front end process.
    """

    def __init__(
        self,
        fe_index: int,
        process: mp.Process,
        input_queue: queue.Queue,
        output_queue: queue.Queue,
    ) -> None:
        self.fe_index = fe_index
        self.process = process
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.message_reader_task: Optional[asyncio.Task] = None
