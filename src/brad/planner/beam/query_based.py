import asyncio
import logging

from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.monitor import Monitor
from brad.planner import BlueprintPlanner
from brad.planner.workload import Workload
from brad.planner.workload.provider import WorkloadProvider
from brad.planner.scoring.performance.analytics_latency import AnalyticsLatencyScorer

logger = logging.getLogger(__name__)


class QueryBasedBeamPlanner(BlueprintPlanner):
    def __init__(
        self,
        current_blueprint: Blueprint,
        current_workload: Workload,
        planner_config: PlannerConfig,
        monitor: Monitor,
        config: ConfigFile,
        schema_name: str,
        workload_provider: WorkloadProvider,
        analytics_latency_scorer: AnalyticsLatencyScorer,
    ) -> None:
        super().__init__()
        self._current_blueprint = current_blueprint
        self._current_workload = current_workload
        self._planner_config = planner_config
        self._monitor = monitor
        self._config = config
        self._schema_name = schema_name

        self._workload_provider = workload_provider
        self._analytics_latency_scorer = analytics_latency_scorer

    async def run_forever(self) -> None:
        while True:
            await asyncio.sleep(3)
            logger.debug("Planner is checking if a replan is needed...")
            if self._check_if_metrics_warrant_replanning():
                await self.run_replan()

    def _check_if_metrics_warrant_replanning(self) -> bool:
        # See if the metrics indicate that we should trigger the planning
        # process.
        return False

    async def run_replan(self) -> None:
        # 1. Need next workload
        # 2. Need to get latency predictions on all three engines
        # 3. Need ability to cluster by tables accessed
        # 4. Need ability to construct blueprints bottom up
        logger.info("Running a replan...")
