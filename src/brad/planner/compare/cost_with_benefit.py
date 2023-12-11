import math
import numpy as np
from datetime import timedelta
from typing import Optional

from .blueprint import ComparableBlueprint
from .function import BlueprintComparator


def best_cost_under_perf_ceilings_with_benefit_horizon(
    max_query_p90_latency_s: float,
    max_txn_p90_latency_s: float,
    curr_query_p90_latency_s: float,
    curr_txn_p90_latency_s: float,
    curr_hourly_cost: float,
    benefit_horizon: timedelta,
    penalty_threshold: float,
    penalty_power: float,
) -> BlueprintComparator:
    def is_better_than(left: ComparableBlueprint, right: ComparableBlueprint) -> bool:
        # Transactional latency ceilings (feasibility check).
        result = _txn_p90_ceiling(left, right, max_txn_p90_latency_s)
        if result is not None:
            return result

        # Query latency ceilings (feasibility check).
        result = _query_p90_ceiling(left, right, max_query_p90_latency_s)
        if result is not None:
            return result

        # Transition times (feasibility check).
        result = _transition_under_benefit_horizon(left, right, benefit_horizon)
        if result is not None:
            return result

        # We compute a scalar score for each blueprint (lower is better):
        #
        # Score = Penalty^Power * C_0 * T + C * (B - T)
        # Penalty = 1 + max(0, max(curr_query_p90 / max_query_p90, curr_txn_p90 / max_txn_p90) - threshold)
        # Threshold = 0.8 (hyperparameter)
        #
        # Notation:
        # C_0 is the operating cost of the current blueprint ($/h)
        # C is the operating cost of the proposed blueprint ($/h)
        # T is the transition time (h)
        # B is the benefit horizon (h)
        #
        # Intuition: The penalty reweighs the first term when we are close to
        # exceeding the performance objectives to encourage selecting blueprints
        # that are faster to transition to.
        penalty_multiplier = _compute_penalty_multiplier(
            max_query_p90_latency_s,
            max_txn_p90_latency_s,
            curr_query_p90_latency_s,
            curr_txn_p90_latency_s,
            penalty_threshold,
        )
        penalty_multiplier = math.pow(penalty_multiplier, penalty_power)

        left_score = _compute_scalar_score(
            left.get_transition_time_s(),
            left.get_transition_cost(),
            curr_hourly_cost,
            left.get_operational_monetary_cost(),
            benefit_horizon,
            penalty_multiplier,
        )
        right_score = _compute_scalar_score(
            right.get_transition_time_s(),
            right.get_transition_cost(),
            curr_hourly_cost,
            right.get_operational_monetary_cost(),
            benefit_horizon,
            penalty_multiplier,
        )

        # For debugging purposes.
        left.set_memoized_value("benefit_penalty_multiplier", penalty_multiplier)
        right.set_memoized_value("benefit_penalty_multiplier", penalty_multiplier)
        left.set_memoized_value("cost_score", left_score)
        right.set_memoized_value("cost_score", right_score)

        return left_score < right_score

    return is_better_than


def _get_or_compute_query_p90_latency(bp: ComparableBlueprint) -> float:
    stored = bp.get_memoized_value("query_p90_latency")
    if stored is not None:
        return stored
    else:
        p90_lat = np.quantile(
            bp.get_predicted_analytical_latencies(), 0.9, method="lower"
        )
        bp.set_memoized_value("query_p90_latency", p90_lat)
        return p90_lat


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


def _query_p90_ceiling(
    left: ComparableBlueprint, right: ComparableBlueprint, query_ceiling_s: float
) -> Optional[bool]:
    # Query latency ceilings.
    left_lat = _get_or_compute_query_p90_latency(left)
    right_lat = _get_or_compute_query_p90_latency(right)

    if left_lat > query_ceiling_s and right_lat > query_ceiling_s:
        # Both are above the latency ceiling.
        # So the better blueprint is the one that does better on performance.
        return left_lat < right_lat
    elif left_lat > query_ceiling_s:
        return False
    elif right_lat > query_ceiling_s:
        return True

    return None


def _transition_under_benefit_horizon(
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


def _compute_penalty_multiplier(
    max_query_p90_latency_s: float,
    max_txn_p90_latency_s: float,
    curr_query_p90_latency_s: float,
    curr_txn_p90_latency_s: float,
    threshold: float,
) -> float:
    query_val = curr_query_p90_latency_s / max_query_p90_latency_s
    txn_val = curr_txn_p90_latency_s / max_txn_p90_latency_s
    return 1.0 + max(0.0, max(query_val, txn_val) - threshold)


def _compute_scalar_score(
    transition_time_s: float,
    transition_cost: float,
    curr_hourly_cost: float,
    next_hourly_cost: float,
    benefit_horizon: timedelta,
    penalty_multiplier: float,
) -> float:
    leftover_time_s = benefit_horizon.total_seconds() - transition_time_s
    assert leftover_time_s > 0.0
    leftover_time_hr = leftover_time_s / 60.0 / 60.0
    transition_time_hr = transition_time_s / 60.0 / 60.0
    # Lower is better.
    return (
        transition_time_hr * curr_hourly_cost * penalty_multiplier
        + transition_cost
        + leftover_time_hr * next_hourly_cost
    )
