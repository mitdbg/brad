import numpy as np
import numpy.typing as npt

from brad.blueprint.provisioning import Provisioning
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.provisioning import (
    aurora_resource_value,
    redshift_resource_value,
)


def scale_aurora_predicted_latency(
    base_predicted_latency: npt.NDArray, to_prov: Provisioning, ctx: ScoringContext
) -> npt.NDArray:
    # predicted = (measured * gamma) * (s/d) * (alpha) + (measured * (1 - gamma))
    # s/d is the ratio
    if to_prov.num_nodes() > 0:
        aurora_predicted = (
            base_predicted_latency
            * ctx.planner_config.aurora_gamma()
            * ctx.planner_config.aurora_alpha()
            * _AURORA_BASE_RESOURCE_VALUE
            / aurora_resource_value(to_prov)
        ) + (base_predicted_latency * (1.0 - ctx.planner_config.aurora_gamma()))
        return aurora_predicted
    else:
        return np.full(base_predicted_latency.shape, np.inf)


def scale_redshift_predicted_latency(
    base_predicted_latency: npt.NDArray, to_prov: Provisioning, ctx: ScoringContext
) -> npt.NDArray:
    if to_prov.num_nodes() > 0:
        redshift_predicted = (
            base_predicted_latency
            * ctx.planner_config.redshift_gamma()
            * ctx.planner_config.redshift_alpha()
            * _REDSHIFT_BASE_RESOURCE_VALUE
            / redshift_resource_value(to_prov)
        ) + (base_predicted_latency * (1.0 - ctx.planner_config.redshift_gamma()))
        return redshift_predicted
    else:
        return np.full(base_predicted_latency.shape, np.inf)


_AURORA_BASE_RESOURCE_VALUE = aurora_resource_value(Provisioning("db.r6i.large", 1))
_REDSHIFT_BASE_RESOURCE_VALUE = redshift_resource_value(Provisioning("dc2.large", 1))
