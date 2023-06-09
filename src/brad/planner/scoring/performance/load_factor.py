from brad.config.engine import Engine
from brad.blueprint.provisioning import Provisioning
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.provisioning import (
    aurora_resource_value,
    redshift_resource_value,
)


def compute_existing_redshift_load_factor(
    curr_cpu_avg: float,
    curr_prov: Provisioning,
    next_prov: Provisioning,
    total_next_latency: float,
    ctx: ScoringContext,
) -> float:
    """
    This is used when Redshift is already running and we are changing its
    provisioning.
    """
    if curr_prov.num_nodes() == 0 or next_prov.num_nodes() == 0:
        # Special case - should be handled differently.
        raise ValueError

    base_latency = ctx.current_latency_weights[Engine.Redshift]
    if base_latency == 0.0:
        # Special case - should be handled differently.
        raise ValueError

    # We scale the predicted query execution times by a factor "l", which is
    # meant to capture the load on the system (e.g., concurrently running
    # queries). We model l as being proportional to the predicted change in CPU
    # utilization across deployments.
    #
    # First we calculate the predicted CPU utilization on the next blueprint
    # (Redshift provisioning and query placement). Then we compute the CPU
    # utilization change and translate this value into l.

    # Query movement scaling factor.
    # Captures change in queries routed to this engine.
    query_factor = total_next_latency / base_latency

    next_cpu = curr_cpu_avg * query_factor

    if curr_prov != next_prov:
        # Resource change scaling factor (inversely proportional).
        curr_val = redshift_resource_value(curr_prov)
        next_val = redshift_resource_value(next_prov)
        resource_ratio = curr_val / next_val

        next_cpu *= (
            resource_ratio  # Captures CPU utilization changes due to a different provisioning
            * ctx.planner_config.redshift_load_resource_alpha()
        )

    if (
        curr_cpu_avg <= ctx.planner_config.redshift_load_min_scaling_cpu()
        and next_cpu <= ctx.planner_config.redshift_load_min_scaling_cpu()
    ):
        # The CPU is predicted to stay below a threshold where there should be
        # no effects on execution time from load.
        return 1.0

    next_cpu = max(next_cpu, ctx.planner_config.redshift_load_min_scaling_cpu())
    starting_cpu = max(curr_cpu_avg, ctx.planner_config.redshift_load_min_scaling_cpu())
    cpu_change = next_cpu / starting_cpu

    return cpu_change * ctx.planner_config.redshift_load_cpu_to_load_alpha()


def compute_existing_aurora_load_factor(
    curr_cpu_avg: float,
    curr_prov: Provisioning,
    next_prov: Provisioning,
    total_next_latency: float,
    ctx: ScoringContext,
) -> float:
    """
    This is used when Aurora is already running and we are changing its
    provisioning.
    """
    if curr_prov.num_nodes() == 0 or next_prov.num_nodes() == 0:
        # Special case - should be handled differently.
        raise ValueError

    base_latency = ctx.current_latency_weights[Engine.Aurora]
    if base_latency == 0.0:
        # Special case - should be handled differently.
        raise ValueError

    # We scale the predicted query execution times by a factor "l", which is
    # meant to capture the load on the system (e.g., concurrently running
    # queries). We model l as being proportional to the predicted change in CPU
    # utilization across deployments.
    #
    # First we calculate the predicted CPU utilization on the next blueprint
    # (Aurora provisioning and query placement). Then we compute the CPU
    # utilization change and translate this value into l.

    # Query movement scaling factor.
    # Captures change in queries routed to this engine.
    query_factor = total_next_latency / base_latency

    next_cpu = curr_cpu_avg * query_factor

    if curr_prov != next_prov:
        # Resource change scaling factor (inversely proportional).
        curr_val = aurora_resource_value(curr_prov)
        next_val = aurora_resource_value(next_prov)
        resource_ratio = curr_val / next_val

        next_cpu *= (
            resource_ratio  # Captures CPU utilization changes due to a different provisioning
            * ctx.planner_config.aurora_load_resource_alpha()
        )

    if (
        curr_cpu_avg <= ctx.planner_config.aurora_load_min_scaling_cpu()
        and next_cpu <= ctx.planner_config.aurora_load_min_scaling_cpu()
    ):
        # The CPU is predicted to stay below a threshold where there should be
        # no effects on execution time from load.
        return 1.0

    next_cpu = max(next_cpu, ctx.planner_config.aurora_load_min_scaling_cpu())
    starting_cpu = max(curr_cpu_avg, ctx.planner_config.aurora_load_min_scaling_cpu())
    cpu_change = next_cpu / starting_cpu

    return cpu_change * ctx.planner_config.aurora_load_cpu_to_load_alpha()
