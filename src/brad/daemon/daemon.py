import asyncio
import signal
import logging
import multiprocessing as mp
from typing import Optional

from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.messages import ShutdownDaemon, NewBlueprint, MetricsReport
from brad.daemon.monitor import Monitor
from brad.data_stats.estimator import Estimator
from brad.data_stats.postgres_estimator import PostgresEstimator
from brad.planner.compare.cost import best_cost_under_geomean_latency
from brad.planner.estimator import EstimatorProvider
from brad.planner.factory import BlueprintPlannerFactory
from brad.planner.metrics import MetricsFromMonitor
from brad.planner.scoring.data_access.provider import DataAccessProvider
from brad.planner.scoring.performance.analytics_latency import AnalyticsLatencyScorer
from brad.planner.workload.provider import WorkloadProvider
from brad.planner.workload import Workload
from brad.routing.policy import RoutingPolicy
from brad.utils import set_up_logging

logger = logging.getLogger(__name__)


class BradDaemon:
    """
    Represents BRAD's background process.

    This code is written with the assumption that this daemon is spawned by the
    BRAD server. In the future, we may want the daemon to be launched
    independently and for it to communicate with the server via RPCs.
    """

    def __init__(
        self,
        config: ConfigFile,
        schema_name: str,
        current_blueprint: Blueprint,
        planner_config: PlannerConfig,
        event_loop: asyncio.AbstractEventLoop,
        input_queue: mp.Queue,
        output_queue: mp.Queue,
    ):
        self._config = config
        self._schema_name = schema_name
        self._planner_config = planner_config
        self._event_loop = event_loop
        self._input_queue = input_queue
        self._output_queue = output_queue

        self._current_blueprint = current_blueprint
        # TODO(Amadou): Determine how to pass in specific clusters.
        self._monitor = Monitor.from_config_file(config)

        self._estimator_provider = _EstimatorProvider()

        self._planner = BlueprintPlannerFactory.create(
            planner_config=planner_config,
            current_blueprint=self._current_blueprint,
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

    async def run_forever(self) -> None:
        """
        Starts running the daemon.
        """
        logger.info("The BRAD daemon is running.")
        if self._config.routing_policy == RoutingPolicy.ForestTableSelectivity:
            estimator = await PostgresEstimator.connect(self._schema_name, self._config)
            await estimator.analyze(self._current_blueprint)
            self._estimator_provider.set_estimator(estimator)

        self._planner.register_new_blueprint_callback(self._handle_new_blueprint)
        self._monitor.force_read_metrics()
        await asyncio.gather(
            self._read_server_messages(),
            self._planner.run_forever(),
            self._monitor.run_forever(),
        )

    async def _read_server_messages(self) -> None:
        """
        Waits for messages from the server and processes them.
        """
        while True:
            message = await self._event_loop.run_in_executor(
                None, self._input_queue.get
            )

            if isinstance(message, ShutdownDaemon):
                logger.debug("Daemon received shutdown message.")
                self._event_loop.create_task(self._shutdown())
                break

            elif isinstance(message, MetricsReport):
                logger.debug(
                    "Received metrics report. txn_completions_per_s: %.2f",
                    message.txn_completions_per_s,
                )
                self._monitor.handle_metric_report(message)

            else:
                logger.debug("Received message %s", str(message))

    async def _handle_new_blueprint(self, blueprint: Blueprint) -> None:
        """
        Informs the server about a new blueprint.
        """
        self._current_blueprint = blueprint
        await self._event_loop.run_in_executor(
            None, self._output_queue.put, NewBlueprint(blueprint)
        )

    async def _shutdown(self) -> None:
        logger.info("The BRAD daemon is shutting down...")
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        # Shut down the estimator too.
        estimator = self._estimator_provider.get_estimator()
        if estimator is not None:
            await estimator.close()

        self._event_loop.stop()

    @staticmethod
    def launch_in_subprocess(
        config_path: str,
        schema_name: str,
        current_blueprint: Blueprint,
        path_to_planner_config: str,
        debug_mode: bool,
        input_queue: mp.Queue,
        output_queue: mp.Queue,
    ) -> None:
        """
        Schedule this method to run in a child process to launch the BRAD
        daemon.
        """
        config = ConfigFile(config_path)
        set_up_logging(filename=config.daemon_log_path, debug_mode=debug_mode)

        planner_config = PlannerConfig(path_to_planner_config)

        event_loop = asyncio.new_event_loop()
        event_loop.set_debug(enabled=debug_mode)
        asyncio.set_event_loop(event_loop)

        # Signal handlers are inherited from the parent server process. We want
        # to ignore these signals since we receive a shutdown signal from the
        # server directly.
        for sig in [signal.SIGTERM, signal.SIGINT]:
            event_loop.add_signal_handler(sig, _noop)

        try:
            daemon = BradDaemon(
                config,
                schema_name,
                current_blueprint,
                planner_config,
                event_loop,
                input_queue,
                output_queue,
            )
            event_loop.create_task(daemon.run_forever())
            logger.info("The BRAD daemon is starting...")
            event_loop.run_forever()
        finally:
            event_loop.close()
            logger.info("The BRAD daemon has shut down.")


def _noop():
    pass


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
