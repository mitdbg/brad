from brad.planner.compare.function import BlueprintComparator
from brad.planner.compare.cost import best_weighted_score_under_perf_ceilings


class BlueprintComparatorProvider:
    """
    Used to customize the comparator used by the blueprint planner. This is
    primarily useful for serialization/deserialization purposes since our
    comparator captures state (it is a closure) and cannot be pickled.
    """

    def get_comparator(self) -> BlueprintComparator:
        raise NotImplementedError


class PerformanceCeilingComparatorProvider(BlueprintComparatorProvider):
    def __init__(
        self, max_query_latency_s: float, max_txn_p90_latency_s: float
    ) -> None:
        self._max_query_latency_s = max_query_latency_s
        self._max_txn_p90_latency_s = max_txn_p90_latency_s

    def get_comparator(self) -> BlueprintComparator:
        return best_weighted_score_under_perf_ceilings(
            self._max_query_latency_s, self._max_txn_p90_latency_s
        )
