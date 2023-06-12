from brad.planner.scoring.context import ScoringContext


def compute_existing_redshift_load_factor(
    curr_cpu_avg: float,
    next_cpu: float,
    ctx: ScoringContext,
) -> float:
    """
    This is used when Redshift is already running and we are changing its
    provisioning.
    """
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
    next_cpu: float,
    ctx: ScoringContext,
) -> float:
    """
    This is used when Aurora is already running and we are changing its
    provisioning.
    """
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
