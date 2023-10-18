import logging
import numpy as np
import numpy.typing as npt
from typing import Dict

from brad.config.engine import Engine
from brad.blueprint.provisioning import Provisioning
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.provisioning import redshift_num_cpus

logger = logging.getLogger(__name__)


class RedshiftProvisioningScore:
    def __init__(
        self,
        scaled_run_times: npt.NDArray,
        overall_cpu_denorm: float,
        for_next_prov: Provisioning,
        debug_values: Dict[str, int | float],
    ) -> None:
        self.scaled_run_times = scaled_run_times
        self.debug_values = debug_values
        self.overall_cpu_denorm = overall_cpu_denorm
        self.for_next_prov = for_next_prov

    @classmethod
    def compute(
        cls,
        base_query_run_times: npt.NDArray,
        curr_prov: Provisioning,
        next_prov: Provisioning,
        ctx: ScoringContext,
    ) -> "RedshiftProvisioningScore":
        """
        Computes all of the Redshift provisioning-dependent scoring components in one
        place.
        """
        # Special case: If we turn off Redshift (set the number of nodes to 0).
        if next_prov.num_nodes() == 0:
            return cls(
                base_query_run_times,
                0.0,
                next_prov,
                {
                    "redshift_query_factor": 0.0,
                },
            )

        overall_forecasted_cpu_util_pct = ctx.metrics.redshift_cpu_avg
        overall_cpu_util_denorm = (
            (overall_forecasted_cpu_util_pct / 100)
            * redshift_num_cpus(curr_prov)
            * curr_prov.num_nodes()
        )

        # 1. Adjust the analytical portion of the system load for query movement.
        if (
            Engine.Redshift not in ctx.current_latency_weights
            or curr_prov.num_nodes() == 0
        ):
            # Special case. We cannot reweigh the queries because nothing in the
            # current workload ran on Redshift.
            query_factor = 1.0
        else:
            # Query movement scaling factor.
            # Captures change in queries routed to this engine.
            base_latency = ctx.current_latency_weights[Engine.Redshift]
            assert base_latency != 0.0
            total_next_latency = base_query_run_times.sum()
            query_factor = total_next_latency / base_latency

        adjusted_cpu_denorm = query_factor * overall_cpu_util_denorm

        # 2. Scale query execution times based on load and provisioning.
        scaled_rt = cls._scale_load_resources(
            base_query_run_times, next_prov, adjusted_cpu_denorm, ctx
        )

        return cls(
            scaled_rt,
            adjusted_cpu_denorm,
            next_prov,
            {
                "redshift_query_factor": query_factor,
            },
        )

    @staticmethod
    def _scale_load_resources(
        base_predicted_latency: npt.NDArray,
        to_prov: Provisioning,
        overall_cpu_denorm: float,
        ctx: ScoringContext,
    ) -> npt.NDArray:
        resource_factor = _REDSHIFT_BASE_RESOURCE_VALUE / (
            redshift_num_cpus(to_prov) * to_prov.num_nodes()
        )
        basis = np.array(
            [
                overall_cpu_denorm * resource_factor,
                overall_cpu_denorm,
                resource_factor,
                1.0,
            ]
        )
        basis = np.square(basis)
        coefs = ctx.planner_config.redshift_scaling_coefs()
        coefs = np.multiply(coefs, basis)
        num_coefs = coefs.shape[0]

        lat_vals = np.expand_dims(base_predicted_latency, axis=1)
        lat_vals = np.repeat(lat_vals, num_coefs, axis=1)

        return np.dot(lat_vals, coefs)

    def copy(self) -> "RedshiftProvisioningScore":
        return RedshiftProvisioningScore(
            self.scaled_run_times,
            self.overall_cpu_denorm,
            self.for_next_prov,
            self.debug_values.copy(),
        )

    def add_debug_values(self, dest: Dict[str, int | float | str]) -> None:
        dest["redshift_cpu_denorm"] = self.overall_cpu_denorm
        dest.update(self.debug_values)


_REDSHIFT_BASE_RESOURCE_VALUE = redshift_num_cpus(Provisioning("dc2.large", 1))
