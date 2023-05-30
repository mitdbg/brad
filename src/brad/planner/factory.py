from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.monitor import Monitor
from brad.planner import BlueprintPlanner
from brad.planner.neighborhood.neighborhood import NeighborhoodSearchPlanner
from brad.planner.beam.query_based import QueryBasedBeamPlanner
from brad.planner.scoring.performance.analytics_latency import AnalyticsLatencyScorer
from brad.planner.strategy import PlanningStrategy
from brad.planner.workload import Workload
from brad.planner.workload.provider import WorkloadProvider


class BlueprintPlannerFactory:
    @staticmethod
    def create(
        planner_config: PlannerConfig,
        current_blueprint: Blueprint,
        current_workload: Workload,
        monitor: Monitor,
        config: ConfigFile,
        schema_name: str,
        workload_provider: WorkloadProvider,
        analytics_latency_scorer: AnalyticsLatencyScorer,
    ) -> BlueprintPlanner:
        strategy = planner_config.strategy()
        if (
            strategy == PlanningStrategy.FullNeighborhood
            or strategy == PlanningStrategy.SampledNeighborhood
        ):
            return NeighborhoodSearchPlanner(
                current_blueprint=current_blueprint,
                current_workload=current_workload,
                planner_config=planner_config,
                monitor=monitor,
                config=config,
                schema_name=schema_name,
                workload_provider=workload_provider,
                analytics_latency_scorer=analytics_latency_scorer,
            )

        elif strategy == PlanningStrategy.QueryBasedBeam:
            return QueryBasedBeamPlanner(
                current_blueprint=current_blueprint,
                current_workload=current_workload,
                planner_config=planner_config,
                monitor=monitor,
                config=config,
                schema_name=schema_name,
                workload_provider=workload_provider,
                analytics_latency_scorer=analytics_latency_scorer,
            )

        else:
            raise ValueError("Unsupported planning strategy: {}".format(str(strategy)))
