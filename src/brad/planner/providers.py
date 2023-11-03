from brad.planner.compare.provider import BlueprintComparatorProvider
from brad.planner.estimator import EstimatorProvider
from brad.planner.metrics import MetricsProvider
from brad.planner.scoring.data_access.provider import DataAccessProvider
from brad.planner.scoring.performance.analytics_latency import AnalyticsLatencyScorer
from brad.planner.triggers.provider import TriggerProvider
from brad.planner.workload.provider import WorkloadProvider
from brad.planner.router_provider import RouterProvider


class BlueprintProviders:
    """
    This is a convenience class used to help avoid passing in a lot of arguments
    into a blueprint planner.
    """

    def __init__(
        self,
        workload_provider: WorkloadProvider,
        analytics_latency_scorer: AnalyticsLatencyScorer,
        comparator_provider: BlueprintComparatorProvider,
        metrics_provider: MetricsProvider,
        data_access_provider: DataAccessProvider,
        estimator_provider: EstimatorProvider,
        trigger_provider: TriggerProvider,
        router_provider: RouterProvider,
    ) -> None:
        self.workload_provider = workload_provider
        self.analytics_latency_scorer = analytics_latency_scorer
        self.comparator_provider = comparator_provider
        self.metrics_provider = metrics_provider
        self.data_access_provider = data_access_provider
        self.estimator_provider = estimator_provider
        self.trigger_provider = trigger_provider
        self.router_provider = router_provider
