import logging
import numpy as np
import numpy.typing as npt
from typing import Dict, TYPE_CHECKING, Optional

from brad.config.engine import Engine
from brad.blueprint.provisioning import Provisioning
from brad.planner.scoring.provisioning import redshift_num_cpus

if TYPE_CHECKING:
    from brad.planner.scoring.context import ScoringContext

logger = logging.getLogger(__name__)


class RedshiftProvisioningScore:
    def __init__(
        self,
        scaled_run_times: npt.NDArray,
        overall_cpu_denorm: float,
        debug_values: Dict[str, int | float],
    ) -> None:
        self.scaled_run_times = scaled_run_times
        self.debug_values = debug_values
        self.overall_cpu_denorm = overall_cpu_denorm

    @classmethod
    def compute(
        cls,
        base_query_run_times: npt.NDArray,
        query_arrival_counts: npt.NDArray,
        curr_prov: Provisioning,
        next_prov: Provisioning,
        ctx: "ScoringContext",
    ) -> "RedshiftProvisioningScore":
        """
        Computes all of the Redshift provisioning-dependent scoring components in one
        place.
        """
        query_factor = cls.query_movement_factor(
            base_query_run_times, query_arrival_counts, ctx
        )
        predicted_cpu_denorm = cls.predict_cpu_denorm(
            curr_prov, next_prov, query_factor, ctx
        )

        # Special case (turning off Redshift).
        if predicted_cpu_denorm == 0.0:
            return cls(
                base_query_run_times,
                0.0,
                {
                    "redshift_query_factor": 0.0,
                },
            )
        else:
            scaled_rt = cls.predict_query_latency_load_resources(
                base_query_run_times, next_prov, predicted_cpu_denorm, ctx
            )
            return cls(
                scaled_rt,
                predicted_cpu_denorm,
                {
                    "redshift_query_factor": query_factor
                    if query_factor is not None
                    else 0.0,
                },
            )

    @classmethod
    def predict_cpu_denorm(
        cls,
        curr_prov: Provisioning,
        next_prov: Provisioning,
        query_factor: Optional[float],
        ctx: "ScoringContext",
    ) -> float:
        """
        Returns the predicted overall denormalized CPU utilization across the
        entire cluster for `next_prov`.
        """
        # 4 cases
        # Redshift off -> Redshift off (no-op)
        # Redshift off -> Redshift on
        # Redshift on -> Redshift on
        # Redshift on -> Redshift off

        curr_on = curr_prov.num_nodes() > 0
        next_on = next_prov.num_nodes() > 0

        # Simple no-op cases.
        if (not curr_on and not next_on) or (curr_on and not next_on):
            return 0.0

        if not curr_on and next_on:
            # Turning on Redshift.
            # We cannot reweigh the queries because nothing in the current
            # workload ran on Redshift. We prime the load with a fraction of the
            # proposed cluster's peak load.
            return (
                redshift_num_cpus(next_prov)
                * next_prov.num_nodes()
                * ctx.planner_config.redshift_initialize_load_fraction()
            )
        else:
            # Redshift is staying on, but there is a potential provisioning
            # change.
            assert curr_on and next_on

            # Special case. Redshift was on, but nothing ran on it and now we
            # want to run queries on it. We use the same load priming approach
            # but on the current cluster.
            if (
                query_factor is None
                and len(ctx.current_query_locations[Engine.Redshift]) > 0
            ):
                return (
                    redshift_num_cpus(curr_prov)
                    * curr_prov.num_nodes()
                    * ctx.planner_config.redshift_initialize_load_fraction()
                )

            curr_cpu_util_denorm = (
                (ctx.metrics.redshift_cpu_avg / 100.0)
                * redshift_num_cpus(curr_prov)
                * curr_prov.num_nodes()
            )

            # We stay conservative here. See `AuroraProvisioningScore`.
            query_factor_clean = 1.0
            if query_factor is not None:
                if query_factor >= 1.0:
                    query_factor_clean = query_factor
                else:
                    min_query_factor = (
                        ctx.planner_config.redshift_min_load_removal_fraction()
                    )
                    query_factor_clean = max(min_query_factor, query_factor)

            return query_factor_clean * curr_cpu_util_denorm

    @classmethod
    def query_movement_factor(
        cls,
        base_query_run_times: npt.NDArray,
        query_arrival_counts: npt.NDArray,
        ctx: "ScoringContext",
    ) -> Optional[float]:
        # Query movement scaling factor.
        # Captures change in queries routed to this engine.
        if Engine.Redshift not in ctx.engine_latency_norm_factor:
            # Special case. We cannot reweigh the queries because nothing in the
            # current workload ran on Redshift (it could have been off).
            return None
        curr_query_run_times = cls.predict_query_latency_resources(
            base_query_run_times, ctx.current_blueprint.redshift_provisioning(), ctx
        )
        norm_factor = ctx.engine_latency_norm_factor[Engine.Redshift]
        assert norm_factor != 0.0
        total_next_latency = np.dot(curr_query_run_times, query_arrival_counts)
        return total_next_latency / norm_factor

    @classmethod
    def predict_query_latency_load_resources(
        cls,
        base_predicted_latency: npt.NDArray,
        to_prov: Provisioning,
        overall_cpu_denorm: float,
        ctx: "ScoringContext",
    ) -> npt.NDArray:
        if base_predicted_latency.shape[0] == 0:
            return base_predicted_latency

        # 1. Compute the impact of the provisioning.
        prov_predicted_latency = cls.predict_query_latency_resources(
            base_predicted_latency, to_prov, ctx
        )

        # 2. Compute the impact of system load.
        mean_service_time = prov_predicted_latency.mean()
        cpu_util = overall_cpu_denorm / (
            redshift_num_cpus(to_prov) * to_prov.num_nodes()
        )
        denom = max(1e-3, 1.0 - cpu_util)  # Want to avoid division by 0.
        wait_sf = cpu_util / denom
        mean_wait_time = (
            mean_service_time
            * wait_sf
            * ctx.planner_config.redshift_new_scaling_alpha()
        )

        # Predicted running time is the query's execution time alone plus the
        # expected wait time (due to system load).
        return prov_predicted_latency + mean_wait_time

    @staticmethod
    def predict_query_latency_resources(
        base_predicted_latency: npt.NDArray,
        to_prov: Provisioning,
        ctx: "ScoringContext",
    ) -> npt.NDArray:
        if base_predicted_latency.shape[0] == 0:
            return base_predicted_latency

        resource_factor = _REDSHIFT_BASE_RESOURCE_VALUE / (
            redshift_num_cpus(to_prov) * to_prov.num_nodes()
        )
        basis = np.array([resource_factor, 1.0])
        coefs = ctx.planner_config.redshift_new_scaling_coefs()
        coefs = np.multiply(coefs, basis)
        num_coefs = coefs.shape[0]
        lat_vals = np.expand_dims(base_predicted_latency, axis=1)
        lat_vals = np.repeat(lat_vals, num_coefs, axis=1)
        return np.dot(lat_vals, coefs)

    @staticmethod
    def scale_load_resources_legacy(
        base_predicted_latency: npt.NDArray,
        to_prov: Provisioning,
        overall_cpu_denorm: float,
        ctx: "ScoringContext",
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
            self.debug_values.copy(),
        )

    def add_debug_values(self, dest: Dict[str, int | float | str]) -> None:
        dest["redshift_predicted_cpu_denorm"] = self.overall_cpu_denorm
        dest.update(self.debug_values)


_REDSHIFT_BASE_RESOURCE_VALUE = redshift_num_cpus(Provisioning("dc2.large", 2))
