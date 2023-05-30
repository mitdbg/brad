from brad.planner.workload import Workload


class AnalyticsLatencyScorer:
    """
    An abstract interface over a component that attaches predicted execution
    latencies for each query in a workload (for blueprint planning purposes).
    """

    def apply_predicted_latencies(self, workload: Workload) -> None:
        """
        Decorates the analytical queries in the provided `Workload` with
        predicted execution latencies.
        """
        raise NotImplementedError
