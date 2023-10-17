import math
import numpy as np

from .blueprint import ComparableBlueprint
from .function import BlueprintComparator


def best_cost_under_geomean_latency(
    geomean_latency_ceiling_s: float,
) -> BlueprintComparator:
    def is_better_than(left: ComparableBlueprint, right: ComparableBlueprint) -> bool:
        left_lat = _get_or_compute_geomean_latency(left)
        right_lat = _get_or_compute_geomean_latency(right)

        if (
            left_lat > geomean_latency_ceiling_s
            and right_lat > geomean_latency_ceiling_s
        ):
            # Both are above the latency ceiling.
            # So the better blueprint is the one that does better on performance.
            return left_lat < right_lat
        elif left_lat > geomean_latency_ceiling_s:
            return False
        elif right_lat > geomean_latency_ceiling_s:
            return True

        # Both are under the latency ceiling.
        left_cost = left.get_operational_monetary_cost() + left.get_transition_cost()
        right_cost = right.get_operational_monetary_cost() + right.get_transition_cost()

        # We treat the cost differences as significant only when they differ by more than 10%.
        max_cost_ratio = _compute_max_ratio(left_cost, right_cost)
        if max_cost_ratio >= 1.1:
            return left_cost < right_cost

        # The two blueprints have similar costs. We now rank by transition time.
        max_trans_ratio = _compute_max_ratio(
            left.get_transition_time_s(), right.get_transition_time_s()
        )
        if max_trans_ratio >= 1.1:
            return left.get_transition_time_s() < right.get_transition_time_s()

        # Rank by performance.
        max_perf_ratio = _compute_max_ratio(left_lat, right_lat)
        return max_perf_ratio >= 1.1 and left_lat < right_lat

    return is_better_than


def best_cost_under_max_latency(max_latency_ceiling_s: float) -> BlueprintComparator:
    def is_better_than(left: ComparableBlueprint, right: ComparableBlueprint) -> bool:
        left_lat = _get_or_compute_max_latency(left)
        right_lat = _get_or_compute_max_latency(right)

        if left_lat > max_latency_ceiling_s and right_lat > max_latency_ceiling_s:
            # Both are above the latency ceiling.
            # So the better blueprint is the one that does better on performance.
            return left_lat < right_lat
        elif left_lat > max_latency_ceiling_s:
            return False
        elif right_lat > max_latency_ceiling_s:
            return True

        # Both are under the latency ceiling.
        left_cost = left.get_operational_monetary_cost() + left.get_transition_cost()
        right_cost = right.get_operational_monetary_cost() + right.get_transition_cost()

        # We treat the cost differences as significant only when they differ by more than 10%.
        max_cost_ratio = _compute_max_ratio(left_cost, right_cost)
        if max_cost_ratio >= 1.1:
            return left_cost < right_cost

        # The two blueprints have similar costs. We now rank by transition time.
        max_trans_ratio = _compute_max_ratio(
            left.get_transition_time_s(), right.get_transition_time_s()
        )
        if max_trans_ratio >= 1.1:
            return left.get_transition_time_s() < right.get_transition_time_s()

        # Rank by performance.
        max_perf_ratio = _compute_max_ratio(left_lat, right_lat)
        return max_perf_ratio >= 1.1 and left_lat < right_lat

    return is_better_than


def best_cost_under_perf_ceilings(
    max_query_latency_s: float,
    # TODO: Use p90 latency.
    max_txn_p50_latency_s: float,
) -> BlueprintComparator:
    def is_better_than(left: ComparableBlueprint, right: ComparableBlueprint) -> bool:
        # Check transactional latency ceilings first.
        left_txn_p50 = left.get_predicted_transactional_latencies()[0]
        right_txn_p50 = right.get_predicted_transactional_latencies()[0]

        # If one of these candidates have NaN predictions, we need to
        # consider other factors. NaN indicates that a prediction is not
        # available (e.g., due to missing metrics).
        if not math.isnan(left_txn_p50) and not math.isnan(right_txn_p50):
            # Both above the ceiling, return the blueprint that does better on
            # performance.
            if (
                left_txn_p50 > max_txn_p50_latency_s
                and right_txn_p50 > max_txn_p50_latency_s
            ):
                return left_txn_p50 < right_txn_p50
            elif left_txn_p50 > max_txn_p50_latency_s:
                return False
            elif right_txn_p50 > max_txn_p50_latency_s:
                return True

        # Query latency ceilings.
        left_lat = _get_or_compute_p99_latency(left)
        right_lat = _get_or_compute_p99_latency(right)

        if left_lat > max_query_latency_s and right_lat > max_query_latency_s:
            # Both are above the latency ceiling.
            # So the better blueprint is the one that does better on performance.
            return left_lat < right_lat
        elif left_lat > max_query_latency_s:
            return False
        elif right_lat > max_query_latency_s:
            return True

        # Both are under the performance ceilings.
        left_cost = left.get_operational_monetary_cost() + left.get_transition_cost()
        right_cost = right.get_operational_monetary_cost() + right.get_transition_cost()

        # We treat the cost differences as significant only when they differ by more than 10%.
        max_cost_ratio = _compute_max_ratio(left_cost, right_cost)
        if max_cost_ratio >= 1.1:
            return left_cost < right_cost

        # The two blueprints have similar costs. We now rank by transition time.
        max_trans_ratio = _compute_max_ratio(
            left.get_transition_time_s(), right.get_transition_time_s()
        )
        if max_trans_ratio >= 1.1:
            return left.get_transition_time_s() < right.get_transition_time_s()

        # Rank by performance.
        max_perf_ratio = _compute_max_ratio(left_lat, right_lat)
        return max_perf_ratio >= 1.1 and left_lat < right_lat

    return is_better_than


def _get_or_compute_geomean_latency(bp: ComparableBlueprint) -> float:
    stored = bp.get_memoized_value("geomean_latency")
    if stored is not None:
        return stored
    else:
        geomean_lat = np.exp(np.log(bp.get_predicted_analytical_latencies()).mean())
        bp.set_memoized_value("geomean_latency", geomean_lat)
        return geomean_lat


def _get_or_compute_max_latency(bp: ComparableBlueprint) -> float:
    stored = bp.get_memoized_value("max_latency")
    if stored is not None:
        return stored
    else:
        max_lat = bp.get_predicted_analytical_latencies().max()
        bp.set_memoized_value("max_latency", max_lat)
        return max_lat


def _get_or_compute_p99_latency(bp: ComparableBlueprint) -> float:
    stored = bp.get_memoized_value("p99_latency")
    if stored is not None:
        return stored
    else:
        p99_lat = np.quantile(
            bp.get_predicted_analytical_latencies(), 0.99, method="lower"
        )
        bp.set_memoized_value("p99_latency", p99_lat)
        return p99_lat


def _compute_max_ratio(value1: float, value2: float) -> float:
    if value1 == 0.0 or value2 == 0.0:
        return np.inf
    return max(value1 / value2, value2 / value1)
