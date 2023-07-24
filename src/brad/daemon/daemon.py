import asyncio
import logging
import queue
import multiprocessing as mp
from typing import Optional, List

from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.blueprint_manager import BlueprintManager
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.messages import ShutdownFrontEnd, Sentinel, MetricsReport
from brad.daemon.monitor import Monitor
from brad.data_stats.estimator import Estimator
from brad.data_stats.postgres_estimator import PostgresEstimator
from brad.front_end.start_front_end import start_front_end
from brad.planner.abstract import BlueprintPlanner
from brad.planner.compare.cost import best_cost_under_geomean_latency
from brad.planner.estimator import EstimatorProvider
from brad.planner.factory import BlueprintPlannerFactory
from brad.planner.metrics import MetricsFromMonitor
from brad.planner.scoring.data_access.provider import DataAccessProvider
from brad.planner.scoring.performance.analytics_latency import AnalyticsLatencyScorer
from brad.planner.workload.provider import WorkloadProvider
from brad.planner.workload import Workload
from brad.routing.policy import RoutingPolicy

logger = logging.getLogger(__name__)


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
        schema_name: str,
        path_to_planner_config: str,
        debug_mode: bool,
    ):
        self._config = config
        self._schema_name = schema_name
        self._path_to_planner_config = path_to_planner_config
        self._planner_config = PlannerConfig(path_to_planner_config)
        self._debug_mode = debug_mode

        self._assets = AssetManager(self._config)
        self._blueprint_mgr = BlueprintManager(self._assets, self._schema_name)
        # TODO(Amadou): Determine how to pass in specific clusters.
        self._monitor = Monitor.from_config_file(config)
        self._estimator_provider = _EstimatorProvider()
        self._planner: Optional[BlueprintPlanner] = None

        self._process_manager: Optional[mp.managers.SyncManager] = None
        self._front_ends: List[_FrontEndProcess] = []

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
        finally:
            logger.info("The BRAD daemon is shutting down...")
            await self._run_teardown()
            logger.info("The BRAD daemon has shut down.")

    async def _run_setup(self) -> None:
        await self._blueprint_mgr.load()
        logger.info("Current blueprint: %s", self._blueprint_mgr.get_blueprint())

        self._planner = BlueprintPlannerFactory.create(
            planner_config=self._planner_config,
            current_blueprint=self._blueprint_mgr.get_blueprint(),
            # N.B. This is a placeholder
            current_workload=Workload.empty(),
            monitor=self._monitor,
            config=self._config,
            schema_name=self._schema_name,
            # TODO: Hook into the query logging infrastructure. This is a placeholder.
            workload_provider=_EmptyWorkloadProvider(),
            # TODO: Hook into the learned performance cost model. This is a placeholder.
            analytics_latency_scorer=_NoopAnalyticsScorer(),
            # TODO: Make this configurable.
            comparator=best_cost_under_geomean_latency(geomean_latency_ceiling_s=10),
            metrics_provider=MetricsFromMonitor(self._monitor, forecasted=True),
            # TODO: Hook into the data access models. This is a placeholder.
            data_access_provider=_NoopDataAccessProvider(),
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

        self._monitor.force_read_metrics()

    async def _run_teardown(self) -> None:
        # Shut down the front end processes.
        # 1. Send a message to tell them to shut down.
        # 2. Input sentinel messages to unblock our reader tasks.
        # 3. Wait for the processes to shut down.
        logger.info("Telling %d front end(s) to shut down...", len(self._front_ends))
        for fe in self._front_ends:
            fe.input_queue.put(ShutdownFrontEnd())
            if fe.message_reader_task is not None:
                fe.output_queue.put(Sentinel())

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
            if isinstance(message, MetricsReport):
                if message.fe_index != front_end.fe_index:
                    logger.warning(
                        "Received message with invalid front end index. Expected %d. Received %d.",
                        front_end.fe_index,
                        message.fe_index,
                    )
                    continue
                self._monitor.handle_metric_report(message)
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
        # TODO: Need to persist the blueprint and notify the front ends to
        # transition.
        logger.info("Planner selected new blueprint: %s", blueprint)


class _EmptyWorkloadProvider(WorkloadProvider):
    def next_workload(self) -> Workload:
        return Workload.empty()


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
