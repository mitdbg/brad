from brad.config.engine import Engine
from brad.blueprint.provisioning import Provisioning
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.provisioning import (
    aurora_resource_value,
    redshift_resource_value,
)


def compute_next_redshift_cpu(
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

    if Engine.Redshift not in ctx.current_latency_weights:
        # Special case. We cannot reweigh the queries because nothing in the
        # current workload ran on Redshift.
        query_factor = 1.0
    else:
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
        base_latency = ctx.current_latency_weights[Engine.Redshift]
        assert base_latency != 0.0
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

    return next_cpu


def compute_next_aurora_cpu(
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

    if Engine.Aurora not in ctx.current_latency_weights:
        # Special case. We cannot reweigh the queries because nothing in the
        # current workload ran on Redshift.
        query_factor = 1.0
    else:
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
        base_latency = ctx.current_latency_weights[Engine.Aurora]
        assert base_latency != 0.0
        query_factor = total_next_latency / base_latency

    next_cpu = curr_cpu_avg * query_factor

    if curr_prov != next_prov:
        # Resource change scaling factor (inversely proportional).
        # Adding more nodes creates read replicas, which should decrease CPU usage.
        curr_val = aurora_resource_value(curr_prov) * curr_prov.num_nodes()
        next_val = aurora_resource_value(next_prov) * next_prov.num_nodes()
        resource_ratio = curr_val / next_val

        next_cpu *= (
            resource_ratio  # Captures CPU utilization changes due to a different provisioning
            * ctx.planner_config.aurora_load_resource_alpha()
        )

    return next_cpu
