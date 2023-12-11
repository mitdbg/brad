from datetime import timedelta

from brad.planner.compare.function import BlueprintComparator
from brad.planner.compare.cost import best_cost_under_perf_ceilings
from brad.planner.compare.cost_with_benefit import (
    best_cost_under_perf_ceilings_with_benefit_horizon,
)
from brad.planner.metrics import Metrics


class BlueprintComparatorProvider:
    """
    Used to customize the comparator used by the blueprint planner. This is
    primarily useful for serialization/deserialization purposes since our
    comparator captures state (it is a closure) and cannot be pickled.
    """

    def get_comparator(
        self, metrics: Metrics, curr_hourly_cost: float
    ) -> BlueprintComparator:
        raise NotImplementedError


class PerformanceCeilingComparatorProvider(BlueprintComparatorProvider):
    def __init__(
        self, max_query_latency_s: float, max_txn_p90_latency_s: float
    ) -> None:
        self._max_query_latency_s = max_query_latency_s
        self._max_txn_p90_latency_s = max_txn_p90_latency_s

    def get_comparator(
        self, metrics: Metrics, curr_hourly_cost: float
    ) -> BlueprintComparator:
        return best_cost_under_perf_ceilings(
            self._max_query_latency_s, self._max_txn_p90_latency_s
        )


class BenefitPerformanceCeilingComparatorProvider(BlueprintComparatorProvider):
    def __init__(
        self,
        max_query_p90_latency_s: float,
        max_txn_p90_latency_s: float,
        benefit_horizon: timedelta,
        threshold: float,
        penalty_power: float,
    ) -> None:
        self._max_query_p90_latency_s = max_query_p90_latency_s
        self._max_txn_p90_latency_s = max_txn_p90_latency_s
        self._benefit_horizon = benefit_horizon
        self._threshold = threshold
        self._penalty_power = penalty_power

    def get_comparator(
        self, metrics: Metrics, curr_hourly_cost: float
    ) -> BlueprintComparator:
        # To support logged planning runs.
        if hasattr(self, "_penalty_power"):
            penalty_power = self._penalty_power
        else:
            penalty_power = 1.0
        return best_cost_under_perf_ceilings_with_benefit_horizon(
            max_query_p90_latency_s=self._max_query_p90_latency_s,
            max_txn_p90_latency_s=self._max_txn_p90_latency_s,
            curr_query_p90_latency_s=metrics.query_lat_s_p90,
            curr_txn_p90_latency_s=metrics.txn_lat_s_p90,
            curr_hourly_cost=curr_hourly_cost,
            benefit_horizon=self._benefit_horizon,
            penalty_threshold=self._threshold,
            penalty_power=penalty_power,
        )
