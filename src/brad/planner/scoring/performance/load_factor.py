from brad.config.engine import Engine
from brad.blueprint.provisioning import Provisioning
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.provisioning import redshift_resource_value


def compute_redshift_load_factor(
    curr_cpu_avg: float,
    curr_prov: Provisioning,
    next_prov: Provisioning,
    total_next_latency: float,
    ctx: ScoringContext,
) -> float:
    if curr_prov.num_nodes() == 0 or next_prov.num_nodes() == 0:
        # Special case - should be handled differently.
        return 0.0

    base_latency = ctx.current_latency_weights[Engine.Redshift]
    if base_latency == 0.0:
        # Special case - should be handled differently.
        return 0.0

    # We scale the predicted query execution times by a factor "l", which is
    # meant to capture the load on the system (e.g., concurrently running
    # queries). We model l as being proportional to the predicted change in CPU
    # utilization across deployments.
    #
    # First we calculate the predicted CPU utilization on the next blueprint
    # (Redshift provisioning and query placement). Then we compute the CPU
    # utilization change and translate this value into l.

    # Query movement scaling factor.
    query_factor = total_next_latency / base_latency

    # Resource change scaling factor (inversely proportional).
    curr_val = redshift_resource_value(curr_prov)
    next_val = redshift_resource_value(next_prov)
    resource_ratio = curr_val / next_val

    next_cpu = (
        curr_cpu_avg
        * query_factor
        * resource_ratio
        * ctx.planner_config.redshift_load_resource_alpha()
    )
    next_cpu = max(next_cpu, ctx.planner_config.redshift_load_min_scaling_cpu())
    cpu_change = next_cpu / curr_cpu_avg

    return cpu_change * ctx.planner_config.redshift_load_cpu_to_load_alpha()
