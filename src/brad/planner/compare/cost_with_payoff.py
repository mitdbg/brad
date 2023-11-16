import math
import numpy as np
from datetime import timedelta
from typing import Optional

from .blueprint import ComparableBlueprint
from .function import BlueprintComparator


def best_cost_under_perf_ceilings_with_payoff(
    max_query_latency_s: float,
    max_txn_p90_latency_s: float,
    payoff_period: timedelta,
    payoff_penalty: float,
    curr_hourly_cost: float,
    curr_blueprint_in_penalty_region: bool,
) -> BlueprintComparator:
    def is_better_than(left: ComparableBlueprint, right: ComparableBlueprint) -> bool:
        # Check transactional latency ceilings first.
        result = _txn_p90_ceiling(left, right, max_txn_p90_latency_s)
        if result is not None:
            return result

        # Query latency ceilings.
        result = _query_ceiling(left, right, max_query_latency_s)
        if result is not None:
            return result

        # Transition times.
        result = _transition_under_payoff_period(left, right, payoff_period)
        if result is not None:
            return result

        if not curr_blueprint_in_penalty_region:
            curr_cost_value = curr_hourly_cost
        else:
            curr_cost_value = payoff_penalty

        left_score = _compute_payoff_cost(
            left.get_transition_time_s(),
            left.get_transition_cost(),
            curr_cost_value,
            left.get_operational_monetary_cost(),
            payoff_period,
        )
        right_score = _compute_payoff_cost(
            right.get_transition_time_s(),
            right.get_transition_cost(),
            curr_cost_value,
            right.get_operational_monetary_cost(),
            payoff_period,
        )
        return left_score < right_score

    return is_better_than


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


def _txn_p90_ceiling(
    left: ComparableBlueprint, right: ComparableBlueprint, txn_lat_s_p90: float
) -> Optional[bool]:
    # Check transactional latency ceilings first.
    left_txn_p90 = left.get_predicted_transactional_latencies()[1]
    right_txn_p90 = right.get_predicted_transactional_latencies()[1]

    # If one of these candidates have NaN predictions, we need to
    # consider other factors. NaN indicates that a prediction is not
    # available (e.g., due to missing metrics).
    if not math.isnan(left_txn_p90) and not math.isnan(right_txn_p90):
        # Both above the ceiling, return the blueprint that does better on
        # performance.
        if left_txn_p90 > txn_lat_s_p90 and right_txn_p90 > txn_lat_s_p90:
            return left_txn_p90 < right_txn_p90
        elif left_txn_p90 > txn_lat_s_p90:
            return False
        elif right_txn_p90 > txn_lat_s_p90:
            return True

    return None


def _query_ceiling(
    left: ComparableBlueprint, right: ComparableBlueprint, query_ceiling_s: float
) -> Optional[bool]:
    # Query latency ceilings.
    left_lat = _get_or_compute_p99_latency(left)
    right_lat = _get_or_compute_p99_latency(right)

    if left_lat > query_ceiling_s and right_lat > query_ceiling_s:
        # Both are above the latency ceiling.
        # So the better blueprint is the one that does better on performance.
        return left_lat < right_lat
    elif left_lat > query_ceiling_s:
        return False
    elif right_lat > query_ceiling_s:
        return True

    return None


def _transition_under_payoff_period(
    left: ComparableBlueprint, right: ComparableBlueprint, payoff_period: timedelta
) -> Optional[bool]:
    left_tr_s = left.get_transition_time_s()
    right_tr_s = right.get_transition_time_s()

    # If either are above the payoff period, return the blueprint that does
    # better on transition time.
    if (
        left_tr_s > payoff_period.total_seconds()
        and right_tr_s > payoff_period.total_seconds()
    ):
        return left_tr_s < right_tr_s
    elif left_tr_s > payoff_period.total_seconds():
        return False
    elif right_tr_s > payoff_period.total_seconds():
        return True

    return None


def _compute_payoff_cost(
    transition_time_s: float,
    transition_cost: float,
    curr_hourly_cost: float,
    next_hourly_cost: float,
    payoff_period: timedelta,
) -> float:
    leftover_time_s = payoff_period.total_seconds() - transition_time_s
    leftover_time_hr = leftover_time_s / 60.0 / 60.0
    transition_time_hr = transition_time_s / 60.0 / 60.0
    # Lower is better.
    return (
        transition_time_hr * curr_hourly_cost
        + transition_cost
        + leftover_time_hr * next_hourly_cost
    )
