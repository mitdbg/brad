from typing import Optional

from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.monitor import Monitor
from brad.daemon.system_event_logger import SystemEventLogger
from brad.planner.abstract import BlueprintPlanner
from brad.planner.compare.function import BlueprintComparator
from brad.planner.estimator import EstimatorProvider
from brad.planner.neighborhood.neighborhood import NeighborhoodSearchPlanner
from brad.planner.beam.query_based import QueryBasedBeamPlanner
from brad.planner.beam.table_based import TableBasedBeamPlanner
from brad.planner.metrics import MetricsProvider
from brad.planner.scoring.score import Score
from brad.planner.scoring.data_access.provider import DataAccessProvider
from brad.planner.scoring.performance.analytics_latency import AnalyticsLatencyScorer
from brad.planner.strategy import PlanningStrategy
from brad.planner.workload.provider import WorkloadProvider


class BlueprintPlannerFactory:
    @staticmethod
    def create(
        planner_config: PlannerConfig,
        current_blueprint: Blueprint,
        current_blueprint_score: Optional[Score],
        monitor: Monitor,
        config: ConfigFile,
        schema_name: str,
        workload_provider: WorkloadProvider,
        analytics_latency_scorer: AnalyticsLatencyScorer,
        comparator: BlueprintComparator,
        metrics_provider: MetricsProvider,
        data_access_provider: DataAccessProvider,
        estimator_provider: EstimatorProvider,
        system_event_logger: Optional[SystemEventLogger] = None,
    ) -> BlueprintPlanner:
        strategy = planner_config.strategy()
        if (
            strategy == PlanningStrategy.FullNeighborhood
            or strategy == PlanningStrategy.SampledNeighborhood
        ):
            return NeighborhoodSearchPlanner(
                current_blueprint=current_blueprint,
                current_blueprint_score=current_blueprint_score,
                planner_config=planner_config,
                monitor=monitor,
                config=config,
                schema_name=schema_name,
                workload_provider=workload_provider,
                analytics_latency_scorer=analytics_latency_scorer,
                comparator=comparator,
                metrics_provider=metrics_provider,
                data_access_provider=data_access_provider,
                estimator_provider=estimator_provider,
                system_event_logger=system_event_logger,
            )

        elif strategy == PlanningStrategy.QueryBasedBeam:
            return QueryBasedBeamPlanner(
                current_blueprint=current_blueprint,
                current_blueprint_score=current_blueprint_score,
                planner_config=planner_config,
                monitor=monitor,
                config=config,
                schema_name=schema_name,
                workload_provider=workload_provider,
                analytics_latency_scorer=analytics_latency_scorer,
                comparator=comparator,
                metrics_provider=metrics_provider,
                data_access_provider=data_access_provider,
                estimator_provider=estimator_provider,
                system_event_logger=system_event_logger,
            )

        elif strategy == PlanningStrategy.TableBasedBeam:
            return TableBasedBeamPlanner(
                current_blueprint=current_blueprint,
                current_blueprint_score=current_blueprint_score,
                planner_config=planner_config,
                monitor=monitor,
                config=config,
                schema_name=schema_name,
                workload_provider=workload_provider,
                analytics_latency_scorer=analytics_latency_scorer,
                comparator=comparator,
                metrics_provider=metrics_provider,
                data_access_provider=data_access_provider,
                estimator_provider=estimator_provider,
                system_event_logger=system_event_logger,
            )

        else:
            raise ValueError("Unsupported planning strategy: {}".format(str(strategy)))
