import logging
import math
import numpy as np
import numpy.typing as npt
from typing import Dict, TYPE_CHECKING, Optional

from brad.config.engine import Engine
from brad.blueprint.provisioning import Provisioning
from brad.planner.scoring.provisioning import redshift_num_cpus
from brad.planner.scoring.performance.queuing import predict_mm1_wait_time

if TYPE_CHECKING:
    from brad.planner.scoring.context import ScoringContext

logger = logging.getLogger(__name__)


class RedshiftProvisioningScore:
    def __init__(
        self,
        scaled_run_times: npt.NDArray,
        max_node_cpu_util: float,
        debug_values: Dict[str, int | float],
    ) -> None:
        self.scaled_run_times = scaled_run_times
        self.debug_values = debug_values
        self.max_node_cpu_util = max_node_cpu_util

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
        predicted_max_node_cpu_util = cls.predict_max_node_cpu_util(
            curr_prov, next_prov, query_factor, ctx
        )

        # Special case (turning off Redshift).
        if predicted_max_node_cpu_util == 0.0:
            return cls(
                base_query_run_times,
                0.0,
                {
                    "redshift_query_factor": 0.0,
                    "redshift_skew_adjustment": np.nan,
                },
            )
        else:
            scaled_rt = cls.predict_query_latency_load_resources(
                base_query_run_times, next_prov, predicted_max_node_cpu_util, ctx
            )
            return cls(
                scaled_rt,
                predicted_max_node_cpu_util,
                {
                    "redshift_query_factor": query_factor
                    if query_factor is not None
                    else np.nan,
                    "redshift_skew_adjustment": ctx.cpu_skew_adjustment
                    if ctx.cpu_skew_adjustment is not None
                    else np.nan,
                },
            )

    @classmethod
    def predict_max_node_cpu_util(
        cls,
        curr_prov: Provisioning,
        next_prov: Provisioning,
        query_factor: Optional[float],
        ctx: "ScoringContext",
    ) -> float:
        """
        Returns the predicted maximum node CPU utilization across the entire
        cluster for `next_prov`.
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
            # We cannot reweigh the load because nothing in the current
            # workload ran on Redshift. We prime the load with a fraction of the
            # proposed cluster's peak load.
            max_peak_util = ctx.planner_config.redshift_initialize_load_fraction()
            return max_peak_util
        else:
            # Redshift is staying on, but there is a potential provisioning
            # change.
            assert curr_on and next_on

            curr_nodes = curr_prov.num_nodes()
            next_nodes = next_prov.num_nodes()

            # Special case. Redshift was on, but nothing ran on it and now we
            # want to run queries on it. We use the same load priming approach
            # but on the current cluster.
            if (
                query_factor is None
                and len(ctx.current_query_locations[Engine.Redshift]) > 0
            ):
                max_peak_util = ctx.planner_config.redshift_initialize_load_fraction()
                return max_peak_util

            curr_cpu_util: npt.NDArray = ctx.metrics.redshift_cpu_list.copy() / 100.0
            assert curr_cpu_util.shape[0] > 0, "Must have Redshift CPU metrics."
            curr_cpu_util.sort()  # In place.
            if ctx.cpu_skew_adjustment is None:
                ctx.cpu_skew_adjustment = cls.compute_skew_adjustment(curr_cpu_util)

            curr_cpu_denorm = curr_cpu_util * redshift_num_cpus(curr_prov)
            curr_max_cpu_denorm = curr_cpu_denorm.max()

            (
                peak_load,
                peak_load_multiplier,
            ) = ctx.planner_config.redshift_peak_load_multiplier()
            if curr_cpu_util.max() > (peak_load / 100.0):
                curr_max_cpu_denorm *= peak_load_multiplier

            # First step: Adjust load based on a change in the number of nodes.
            # Key observation is that we're interested in the node with the
            # maximum load. This node will stay the maximum after our
            # adjustments because we always multiply by a positive constant
            # (linear scaling) or add the same value to each node.
            if next_nodes > curr_nodes:
                # When this value is close to 0, it indicates high load skew.
                # Thus adding an instance of the same kind of node should not
                # affect the load as much.
                if ctx.cpu_skew_adjustment < 0.5:
                    next_max_cpu_denorm = curr_max_cpu_denorm
                else:
                    next_max_cpu_denorm = curr_max_cpu_denorm * math.pow(
                        curr_nodes / next_nodes, ctx.cpu_skew_adjustment
                    )
            elif next_nodes < curr_nodes:
                removed_nodes = curr_nodes - next_nodes
                load_to_redist = curr_cpu_denorm[:removed_nodes].sum()
                next_max_cpu_denorm = curr_max_cpu_denorm + (
                    load_to_redist / next_nodes
                )
            else:
                # Number of nodes unchanged.
                next_max_cpu_denorm = curr_max_cpu_denorm

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

            # We divide by the CPU count on the next provisioning to adjust for
            # instance type changes.
            next_util = (query_factor_clean * next_max_cpu_denorm) / redshift_num_cpus(
                next_prov
            )

            # Clip to [0, 1].
            return min(max(next_util, 0.0), 1.0)

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
        max_node_cpu_util: float,
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
        # Note the use of p90. The predictions we make are specifically p90 latency.
        wait_time = predict_mm1_wait_time(
            mean_service_time_s=mean_service_time,
            utilization=max_node_cpu_util,
            quantile=0.9,
        )
        # Predicted running time is the query's execution time alone plus the
        # expected wait time (due to system load).
        return prov_predicted_latency + wait_time

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
            self.max_node_cpu_util,
            self.debug_values.copy(),
        )

    def add_debug_values(self, dest: Dict[str, int | float | str]) -> None:
        dest["redshift_predicted_max_node_cpu_util"] = self.max_node_cpu_util
        dest.update(self.debug_values)

    @classmethod
    def compute_skew_adjustment(cls, cpu_utils: npt.NDArray) -> float:
        """
        Returns a value between 0 and 1 where 0 represents maximum CPU
        utilization skew and 1 represents no skew (equal CPU utilization among
        entries).
        """
        num_entries = cpu_utils.shape[0]
        if num_entries <= 1:
            return 1.0

        cpu_utils = cpu_utils.copy()
        cpu_utils.sort()  # In place, ascending.
        max_val = cpu_utils.max()
        rest = cpu_utils[:-1]
        diff_from_max = np.sqrt(np.square(rest - max_val).mean())

        # We want no skew to be a factor of 1.0. Because this takes in CPU
        # utils, the maximum possible diff is 1.0.
        return 1.0 - diff_from_max


_REDSHIFT_BASE_PROV = Provisioning("dc2.large", 2)
_REDSHIFT_BASE_RESOURCE_VALUE = (
    redshift_num_cpus(_REDSHIFT_BASE_PROV) * _REDSHIFT_BASE_PROV.num_nodes()
)
