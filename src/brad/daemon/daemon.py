import asyncio
import logging
import queue
import os
import pathlib
import multiprocessing as mp
import numpy as np
from typing import Optional, List, Set, Tuple

from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.blueprint.diff.blueprint import BlueprintDiff
from brad.blueprint.manager import BlueprintManager
from brad.blueprint.provisioning import Provisioning
from brad.blueprint.state import TransitionState
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.config.system_event import SystemEvent
from brad.config.temp_config import TempConfig
from brad.connection.factory import ConnectionFactory
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
from brad.daemon.system_event_logger import SystemEventLogger
from brad.daemon.transition_orchestrator import TransitionOrchestrator
from brad.daemon.blueprint_watchdog import BlueprintWatchdog
from brad.daemon.populate_stub import create_tables_in_stub, load_tables_in_stub
from brad.data_stats.estimator import Estimator
from brad.data_stats.postgres_estimator import PostgresEstimator
from brad.data_stats.stub_estimator import StubEstimator
from brad.data_sync.execution.executor import DataSyncExecutor
from brad.front_end.start_front_end import start_front_end
from brad.planner.abstract import BlueprintPlanner
from brad.planner.compare.provider import (
    BlueprintComparatorProvider,
    PerformanceCeilingComparatorProvider,
    BenefitPerformanceCeilingComparatorProvider,
)
from brad.planner.enumeration.blueprint import EnumeratedBlueprint
from brad.planner.estimator import EstimatorProvider
from brad.planner.factory import BlueprintPlannerFactory
from brad.planner.metrics import WindowedMetricsFromMonitor
from brad.planner.providers import BlueprintProviders
from brad.planner.scoring.score import Score
from brad.planner.scoring.data_access.provider import DataAccessProvider
from brad.planner.scoring.data_access.precomputed_values import (
    PrecomputedDataAccessProvider,
)
from brad.planner.scoring.performance.analytics_latency import AnalyticsLatencyScorer
from brad.planner.scoring.performance.precomputed_predictions import (
    PrecomputedPredictions,
)
from brad.planner.triggers.provider import ConfigDefinedTriggers
from brad.planner.triggers.trigger import Trigger
from brad.planner.workload import Workload
from brad.planner.workload.builder import WorkloadBuilder
from brad.planner.workload.provider import LoggedWorkloadProvider
from brad.routing.policy import RoutingPolicy
from brad.row_list import RowList
from brad.utils.time_periods import period_start, universal_now
from brad.ui.manager import UiManager

logger = logging.getLogger(__name__)

# Temporarily used.
PERSIST_BLUEPRINT_VAR = "BRAD_PERSIST_BLUEPRINT"
IGNORE_ALL_BLUEPRINTS_VAR = "BRAD_IGNORE_BLUEPRINT"
LOG_ANA_UP_VAR = "BRAD_LOG_ANA_UP_FINAL_BLUEPRINT"


class BradDaemon:
    """
    Represents BRAD's controller process.

    This code is written with the assumption that this daemon spawns the BRAD
    front end servers. In the future, we may want the servers to be launched
    independently and for them to communicate with the daemon via RPCs.
    """

    def __init__(
        self,
        config: ConfigFile,
        temp_config: Optional[TempConfig],
        schema_name: str,
        path_to_system_config: str,
        debug_mode: bool,
        start_ui: bool,
    ):
        self._config = config
        self._temp_config = temp_config
        self._schema_name = schema_name
        self._path_to_system_config = path_to_system_config
        self._planner_config = PlannerConfig.load_from_new_configs(
            system_config=path_to_system_config
        )
        self._debug_mode = debug_mode
        self._start_ui = start_ui

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

        self._system_event_logger = SystemEventLogger.create_if_requested(self._config)
        self._watchdog = BlueprintWatchdog(self._system_event_logger)

        # This is used to hold references to internal command tasks we create.
        # https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
        self._internal_command_tasks: Set[asyncio.Task] = set()

        self._startup_timestamp = universal_now()

        if self._start_ui and UiManager.is_supported():
            self._ui_mgr: Optional[UiManager] = UiManager.create(
                self._config, self._monitor, self._blueprint_mgr
            )
        else:
            self._ui_mgr = None
            if self._start_ui:
                logger.warning(
                    "Cannot start the BRAD UI because it is not supported. "
                    "Please make sure you install BRAD with the [ui] option."
                )

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
            additional_tasks = []
            if self._ui_mgr is not None:
                additional_tasks.append(self._ui_mgr.serve_forever())
            logger.info("The BRAD daemon is running.")
            if self._system_event_logger is not None:
                self._system_event_logger.log(SystemEvent.StartUp)
            await asyncio.gather(
                self._planner.run_forever(),
                self._monitor.run_forever(),
                *message_reader_tasks,
                *additional_tasks,
            )
        except Exception:
            logger.exception("The BRAD daemon encountered an unexpected exception.")
            raise
        finally:
            logger.info("The BRAD daemon is shutting down...")
            await self._run_teardown()
            if self._system_event_logger is not None:
                self._system_event_logger.log(SystemEvent.ShutDown)
            logger.info("The BRAD daemon has shut down.")

    async def _run_setup(self) -> None:
        is_stub_mode = self._config.stub_mode_path() is not None
        await self._blueprint_mgr.load()
        logger.info("Current blueprint: %s", self._blueprint_mgr.get_blueprint())
        if not is_stub_mode:
            logger.info("Current directory: %s", self._blueprint_mgr.get_directory())

        if is_stub_mode:
            stub_conn = ConnectionFactory.connect_to_stub(self._config)
            create_tables_in_stub(
                self._config, stub_conn, self._blueprint_mgr.get_blueprint()
            )
            load_tables_in_stub(
                self._config, stub_conn, self._blueprint_mgr.get_blueprint()
            )
            stub_conn.close_sync()

        # Initialize the monitor.
        self._monitor.set_up_metrics_sources()

        if self._config.data_sync_period_seconds > 0:
            self._timed_sync_task = asyncio.create_task(self._run_sync_periodically())
        await self._data_sync_executor.establish_connections()

        if self._temp_config is not None:
            # TODO: Actually call into the models. We avoid doing so for now to
            # avoid having to implement model loading, etc.
            std_datasets = self._temp_config.std_datasets()
            if len(std_datasets) > 0:
                datasets: List[Tuple[str, str | pathlib.Path]] = [
                    (dataset["name"], dataset["path"]) for dataset in std_datasets
                ]
                latency_scorer: AnalyticsLatencyScorer = (
                    PrecomputedPredictions.load_from_standard_dataset(datasets)
                )
                data_access_provider: DataAccessProvider = (
                    PrecomputedDataAccessProvider.load_from_standard_dataset(datasets)
                )
            else:
                latency_scorer = PrecomputedPredictions.load(
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

            if self._temp_config.comparator_type() == "benefit_perf_ceiling":
                comparator_provider: BlueprintComparatorProvider = (
                    BenefitPerformanceCeilingComparatorProvider(
                        self._temp_config.query_latency_p90_ceiling_s(),
                        self._temp_config.txn_latency_p90_ceiling_s(),
                        self._temp_config.benefit_horizon(),
                        self._temp_config.penalty_threshold(),
                        self._temp_config.penalty_power(),
                    )
                )
            else:
                comparator_provider = PerformanceCeilingComparatorProvider(
                    self._temp_config.query_latency_p90_ceiling_s(),
                    self._temp_config.txn_latency_p90_ceiling_s(),
                )
        else:
            logger.warning(
                "TempConfig not provided. The planner will not be able to run correctly."
            )
            latency_scorer = _NoopAnalyticsScorer()
            data_access_provider = _NoopDataAccessProvider()
            comparator_provider = PerformanceCeilingComparatorProvider(30.0, 0.030)

        # Update just to get the most recent startup time.
        self._startup_timestamp = universal_now()

        providers = BlueprintProviders(
            workload_provider=LoggedWorkloadProvider(
                self._config,
                self._planner_config,
                self._blueprint_mgr,
                self._schema_name,
                self._startup_timestamp,
            ),
            analytics_latency_scorer=latency_scorer,
            comparator_provider=comparator_provider,
            metrics_provider=WindowedMetricsFromMonitor(
                self._monitor,
                self._blueprint_mgr,
                self._config,
                self._planner_config,
                self._startup_timestamp,
            ),
            data_access_provider=data_access_provider,
            estimator_provider=self._estimator_provider,
            trigger_provider=ConfigDefinedTriggers(
                self._config,
                self._planner_config,
                self._monitor,
                data_access_provider,
                self._estimator_provider,
                self._startup_timestamp,
            ),
        )
        self._planner = BlueprintPlannerFactory.create(
            config=self._config,
            planner_config=self._planner_config,
            schema_name=self._schema_name,
            current_blueprint=self._blueprint_mgr.get_blueprint(),
            current_blueprint_score=self._blueprint_mgr.get_active_score(),
            providers=providers,
            system_event_logger=self._system_event_logger,
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
                    self._config,
                    self._schema_name,
                    self._path_to_system_config,
                    self._debug_mode,
                    self._blueprint_mgr.get_directory(),
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

        if (
            self._config.routing_policy == RoutingPolicy.ForestTableSelectivity
            or self._config.routing_policy == RoutingPolicy.Default
        ):
            logger.info("Setting up the cardinality estimator...")
            if is_stub_mode:
                estimator: Estimator = StubEstimator()
            else:
                estimator = await PostgresEstimator.connect(
                    self._schema_name, self._config
                )
            await estimator.analyze(
                self._blueprint_mgr.get_blueprint(),
                # N.B. Only the daemon attempts to repopulate the cache.
                populate_cache_if_missing=True,
            )
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
            try:
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
                    task = asyncio.create_task(
                        self._run_internal_command_request_response(message)
                    )
                    self._internal_command_tasks.add(task)
                    task.add_done_callback(self._internal_command_tasks.discard)

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

                    logger.info(
                        "Received blueprint ack. Version: %d, Front end: %d",
                        message.version,
                        message.fe_index,
                    )

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
            except Exception as ex:
                if not isinstance(ex, asyncio.CancelledError):
                    logger.exception(
                        "Unexpected error when handling front end message. Front end: %d",
                        front_end.fe_index,
                    )

    async def _handle_new_blueprint(
        self, blueprint: Blueprint, score: Score, trigger: Optional[Trigger]
    ) -> None:
        """
        Informs the server about a new blueprint.
        """

        if (
            IGNORE_ALL_BLUEPRINTS_VAR in os.environ
            or self._config.stub_mode_path() is not None
        ):
            logger.info("Skipping all blueprints. Chosen blueprint: %s", blueprint)
            return

        if self._system_event_logger is not None:
            self._system_event_logger.log(SystemEvent.NewBlueprintProposed)

        if self._should_skip_blueprint(blueprint, score, trigger):
            if self._system_event_logger is not None:
                self._system_event_logger.log(SystemEvent.NewBlueprintSkipped)
            return

        if self._watchdog.reject_blueprint(blueprint):
            logger.warning(
                "Blueprint watchdog fired! Must re-run this blueprint planning pass."
            )
            return

        if PERSIST_BLUEPRINT_VAR in os.environ:
            logger.info(
                "Force-persisting the new blueprint. Run a manual transition and "
                "then restart BRAD to load the new blueprint."
            )
            new_version = await self._blueprint_mgr.start_transition(blueprint, score)
            if self._system_event_logger is not None:
                self._system_event_logger.log(
                    SystemEvent.NewBlueprintAccepted, "version={}".format(new_version)
                )
        else:
            logger.info(
                "Planner selected a new blueprint. Transition is starting. New blueprint: %s",
                blueprint,
            )
            new_version = await self._blueprint_mgr.start_transition(blueprint, score)
            if self._system_event_logger is not None:
                self._system_event_logger.log(
                    SystemEvent.NewBlueprintAccepted, "version={}".format(new_version)
                )
            if self._planner is not None:
                self._planner.set_disable_triggers(disable=True)
            self._transition_orchestrator = TransitionOrchestrator(
                self._config, self._blueprint_mgr, self._system_event_logger
            )
            self._transition_task = asyncio.create_task(self._run_transition_part_one())

    def _should_skip_blueprint(
        self, blueprint: Blueprint, score: Score, _trigger: Optional[Trigger]
    ) -> bool:
        """
        This is called whenever the planner chooses a new blueprint. The purpose
        is to avoid transitioning to blueprints with few changes.

        We always skip the blueprint if a transition is currently in progress.

        We do not skip the blueprint if:
        - If there is a provisioning change
        - The change in query routing is above a threshold
        """
        if self._transition_orchestrator is not None:
            logger.warning(
                "Planner selected a new blueprint, but we are still transitioning to a blueprint. Skipping new blueprint: %s",
                blueprint,
            )
            return True

        current_blueprint = self._blueprint_mgr.get_blueprint()
        diff = BlueprintDiff.of(current_blueprint, blueprint)
        if diff is None:
            logger.info("Planner selected an identical blueprint - skipping.")
            return True

        current_score = self._blueprint_mgr.get_active_score()
        if current_score is None:
            # Do not skip - we are currently missing the score of the active
            # blueprint, so there is nothing to compare to.
            return False

        if diff.aurora_diff() is not None or diff.redshift_diff() is not None:
            return False

        current_dist = current_score.normalized_query_count_distribution()
        next_dist = score.normalized_query_count_distribution()
        abs_delta = np.abs(next_dist - current_dist).sum()

        if abs_delta >= self._planner_config.query_dist_change_frac():
            return False
        else:
            logger.info(
                "Skipping blueprint because the query distribution change (%.4f) falls under the threshold (%.4f).",
                abs_delta,
                self._planner_config.query_dist_change_frac(),
            )
            return True

    async def _run_sync_periodically(self) -> None:
        while True:
            await asyncio.sleep(self._config.data_sync_period_seconds)
            logger.debug("Starting an auto data sync.")
            await self._data_sync_executor.run_sync(self._blueprint_mgr.get_blueprint())

    async def _run_internal_command_request_response(
        self, msg: InternalCommandRequest
    ) -> None:
        try:
            results = await self._handle_internal_command(msg.request)
            response = InternalCommandResponse(msg.fe_index, results)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, self._front_ends[msg.fe_index].input_queue.put, response
            )
        except Exception as ex:
            logger.exception(
                "Unexpected exception when handling internal command: %s", msg.request
            )
            await loop.run_in_executor(
                None,
                self._front_ends[msg.fe_index].input_queue.put,
                InternalCommandResponse(msg.fe_index, [(str(ex),)]),
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
            now = universal_now()
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
            if window_start < self._startup_timestamp:
                window_start = period_start(
                    self._startup_timestamp, self._config.epoch_length
                )
                logger.info(
                    "Adjusting lookback window to start at system startup: %s",
                    self._startup_timestamp.strftime("%Y-%m-%d %H:%M:%S,%f"),
                )
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
            if self._system_event_logger is not None:
                self._system_event_logger.log(SystemEvent.ManuallyTriggeredReplan)
            try:
                await self._planner.run_replan(
                    trigger=None, window_multiplier=window_multiplier
                )
                return [("Planner completed. See the daemon's logs for more details.",)]
            except Exception as ex:
                logger.exception("Encountered exception when running the planner.")
                return [(str(ex),)]

        elif command.startswith("BRAD_MODIFY_REDSHIFT"):
            parts = command.split(" ")
            if len(parts) <= 1:
                return [("Nothing to modify.",)]

            new_num_nodes = int(parts[1])
            logger.info("Setting Redshift to dc2.large(%d)", new_num_nodes)

            curr_blueprint = self._blueprint_mgr.get_blueprint()
            ebp = EnumeratedBlueprint(curr_blueprint)
            ebp.set_redshift_provisioning(Provisioning("dc2.large", new_num_nodes))
            new_blueprint = ebp.to_blueprint()
            new_version = await self._blueprint_mgr.start_transition(
                new_blueprint, new_score=None
            )
            if self._system_event_logger is not None:
                self._system_event_logger.log(
                    SystemEvent.NewBlueprintAccepted, "version={}".format(new_version)
                )
            if self._planner is not None:
                self._planner.set_disable_triggers(disable=True)
            self._transition_orchestrator = TransitionOrchestrator(
                self._config, self._blueprint_mgr, self._system_event_logger
            )
            self._transition_task = asyncio.create_task(self._run_transition_part_one())
            return [("Transition in progress.",)]

        elif command.startswith("BRAD_USE_PRESET_BP"):
            parts = command.split(" ")
            if len(parts) <= 1:
                return [("Need to specify a preset.",)]

            preset = parts[1].lower()
            if not preset in ["dl_lo", "dl_med", "dl_hi"]:
                return [(f"Unrecognized preset {preset}",)]

            curr_blueprint = self._blueprint_mgr.get_blueprint()
            ebp = EnumeratedBlueprint(curr_blueprint)

            if preset == "dl_lo":
                ebp.set_aurora_provisioning(Provisioning("db.t4g.medium", 2))
                ebp.set_redshift_provisioning(Provisioning("dc2.large", 4))

            elif preset == "dl_med":
                ebp.set_aurora_provisioning(Provisioning("db.r6g.xlarge", 1))
                ebp.set_redshift_provisioning(Provisioning("dc2.large", 8))

            elif preset == "dl_hi":
                ebp.set_aurora_provisioning(Provisioning("db.r6g.xlarge", 1))
                ebp.set_redshift_provisioning(Provisioning("dc2.large", 16))

            new_blueprint = ebp.to_blueprint()
            new_version = await self._blueprint_mgr.start_transition(
                new_blueprint, new_score=None
            )
            if self._system_event_logger is not None:
                self._system_event_logger.log(
                    SystemEvent.NewBlueprintAccepted, "version={}".format(new_version)
                )
            if self._planner is not None:
                self._planner.set_disable_triggers(disable=True)
            self._transition_orchestrator = TransitionOrchestrator(
                self._config, self._blueprint_mgr, self._system_event_logger
            )
            self._transition_task = asyncio.create_task(self._run_transition_part_one())
            return [(f"Transition to {preset} in progress.",)]

        else:
            logger.warning("Received unknown internal command: %s", command)
            return []

    async def _run_transition_part_one(self) -> None:
        try:
            assert self._transition_orchestrator is not None
            tm = self._blueprint_mgr.get_transition_metadata()
            transitioning_to_version = tm.next_version

            if self._system_event_logger is not None:
                next_blueprint = tm.next_blueprint
                assert next_blueprint is not None
                next_aurora = str(next_blueprint.aurora_provisioning())
                next_redshift = str(next_blueprint.redshift_provisioning())

                self._system_event_logger.log(
                    SystemEvent.PreTransitionStarted,
                    f"version={transitioning_to_version};"
                    f"aurora={next_aurora};"
                    f"redshift={next_redshift}",
                )

            def update_monitor_sources():
                self._monitor.update_metrics_sources()

            await self._transition_orchestrator.run_prepare_then_transition(
                update_monitor_sources
            )

            # Switch to the transitioned state.
            tm = self._blueprint_mgr.get_transition_metadata()
            assert (
                tm.state == TransitionState.TransitionedPreCleanUp
            ), "Incorrect transition state."
            assert tm.next_version is not None, "Missing next version."

            directory = self._blueprint_mgr.get_directory()
            logger.info(
                "Switched to new directory during blueprint transition: %s", directory
            )
            self._monitor.update_metrics_sources()

            await self._data_sync_executor.update_connections()

            if self._system_event_logger is not None:
                self._system_event_logger.log(
                    SystemEvent.PreTransitionCompleted,
                    "version={}".format(transitioning_to_version),
                )

            # Inform all front ends about the new blueprint.
            logger.debug(
                "Notifying %d front ends about the new blueprint.",
                len(self._front_ends),
            )
            self._transition_orchestrator.set_waiting_for_front_ends(
                len(self._front_ends)
            )
            for fe in self._front_ends:
                fe.input_queue.put(
                    NewBlueprint(
                        fe.fe_index,
                        tm.next_version,
                        self._blueprint_mgr.get_directory(),
                    )
                )

            self._transition_task = None

            # We finish the transition after all front ends acknowledge that they
            # have transitioned to the new blueprint.
        except:  # pylint: disable=bare-except
            # Because this runs as a background asyncio task, we log any errors
            # that occur so that failures are not silent.
            logger.exception(
                "Transition part one failed due to an unexpected exception."
            )

    async def _run_transition_part_two(self) -> None:
        try:
            assert self._transition_orchestrator is not None
            tm = self._blueprint_mgr.get_transition_metadata()
            transitioning_to_version = tm.next_version
            if self._system_event_logger is not None:
                self._system_event_logger.log(
                    SystemEvent.PostTransitionStarted,
                    "version={}".format(transitioning_to_version),
                )

            await self._transition_orchestrator.run_clean_up_after_transition()
            if self._planner is not None:
                self._planner.update_blueprint(
                    self._blueprint_mgr.get_blueprint(),
                    self._blueprint_mgr.get_active_score(),
                )

            # Done.
            tm = self._blueprint_mgr.get_transition_metadata()
            assert (
                tm.state == TransitionState.Stable
            ), "Incorrect transition state after completion."
            if self._planner is not None:
                self._planner.set_disable_triggers(disable=False)
            logger.info(
                "Completed the transition to blueprint version %d", tm.curr_version
            )
            self._transition_task = None
            self._transition_orchestrator = None

            if self._system_event_logger is not None:
                self._system_event_logger.log(
                    SystemEvent.PostTransitionCompleted,
                    "version={}".format(transitioning_to_version),
                )

                new_blueprint = self._blueprint_mgr.get_blueprint()
                new_redshift = new_blueprint.redshift_provisioning()
                # TODO: Should change this as needed. This is to signal to the
                # experiment runner when we have completed the workload.
                if LOG_ANA_UP_VAR in os.environ and (
                    (
                        new_redshift.instance_type() == "ra3.xlplus"
                        and new_redshift.num_nodes() >= 8
                    )
                    or (
                        new_redshift.instance_type() == "ra3.4xlarge"
                        and new_redshift.num_nodes() > 1
                    )
                ):
                    self._system_event_logger.log(
                        SystemEvent.ReachedExpectedState,
                        "redshift={}".format(str(new_redshift)),
                    )
        except:  # pylint: disable=bare-except
            logger.exception(
                "Transition part two failed due to an unexpected exception."
            )


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
