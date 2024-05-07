import logging
import math
import numpy as np
import numpy.typing as npt
from typing import Dict, TYPE_CHECKING, Optional, Iterator, List, Tuple, Any

from brad.config.engine import Engine
from brad.daemon.hot_config import HotConfig
from brad.blueprint.provisioning import Provisioning
from brad.planner.scoring.provisioning import redshift_num_cpus
from brad.planner.scoring.performance.queuing import predict_mm1_wait_time
from brad.planner.workload import Workload

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
        query_indices: List[int],
        workload: Workload,
        curr_prov: Provisioning,
        next_prov: Provisioning,
        ctx: "ScoringContext",
    ) -> "RedshiftProvisioningScore":
        """
        Computes all of the Redshift provisioning-dependent scoring components in one
        place.
        """
        debug_dict: Dict[str, Any] = {}
        query_factor = cls.query_movement_factor(query_indices, workload, ctx)
        total_cpu_denorm, max_per_query_cpu_denorm = cls.compute_direct_cpu_denorm(
            query_indices, workload, next_prov, ctx, debug_dict
        )

        # Load adjustment factor.
        # TODO: Hardcoded SLO.
        if (
            ctx.metrics.redshift_cpu_list is not None
            and ctx.metrics.redshift_cpu_list.shape[0] > 0
        ):
            avg_cpu: float = ctx.metrics.redshift_cpu_list.mean().item()
        else:
            # This won't be used. This is actually max.
            avg_cpu = float(ctx.metrics.redshift_cpu_avg)

        gamma_norm_factor = HotConfig.instance().get_value(
            "query_lat_p90", default=30.0
        )
        gamma = (
            min(ctx.metrics.query_lat_s_p90 / gamma_norm_factor + 0.35, 1.0)
            if avg_cpu >= 90.0
            else 1.0
        )
        debug_dict["redshift_gamma_factor"] = gamma
        if (
            ctx.metrics.redshift_cpu_list is not None
            and ctx.metrics.redshift_cpu_list.shape[0] > 0
        ):
            debug_dict["redshift_effective_cpu_util"] = (
                gamma * ctx.metrics.redshift_cpu_list.max()
            )

        predicted_max_node_cpu_util = cls.predict_max_node_cpu_util(
            curr_prov,
            next_prov,
            query_factor,
            total_cpu_denorm,
            max_per_query_cpu_denorm,
            gamma,
            ctx,
        )

        # Special case (turning off Redshift).
        if predicted_max_node_cpu_util == 0.0:
            return cls(
                workload.get_predicted_analytical_latency_batch(
                    query_indices, Engine.Redshift
                ),
                0.0,
                {
                    **debug_dict,
                    "redshift_query_factor": 0.0,
                    "redshift_skew_adjustment": np.nan,
                },
            )
        else:
            scaled_rt = cls.predict_query_latency_load_resources(
                query_indices, workload, next_prov, predicted_max_node_cpu_util
            )
            return cls(
                scaled_rt,
                predicted_max_node_cpu_util,
                {
                    **debug_dict,
                    "redshift_query_factor": (
                        query_factor if query_factor is not None else np.nan
                    ),
                    "redshift_skew_adjustment": (
                        ctx.cpu_skew_adjustment
                        if ctx.cpu_skew_adjustment is not None
                        else np.nan
                    ),
                },
            )

    @classmethod
    def predict_max_node_cpu_util(
        cls,
        curr_prov: Provisioning,
        next_prov: Provisioning,
        query_factor: Optional[float],
        total_cpu_denorm: float,
        max_per_query_cpu_denorm: float,
        gamma: float,
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
            # workload ran on Redshift. We use the computed CPU denorm value.
            return total_cpu_denorm / redshift_num_cpus(next_prov)
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
                return total_cpu_denorm / redshift_num_cpus(next_prov)

            curr_cpu_util: npt.NDArray = ctx.metrics.redshift_cpu_list.copy() / 100.0
            if curr_cpu_util.shape[0] == 0:
                # This is to support running recorded legacy planning runs.
                curr_cpu_util = np.ones(curr_nodes) * ctx.metrics.redshift_cpu_avg
            curr_cpu_util.sort()  # In place.
            if ctx.cpu_skew_adjustment is None:
                ctx.cpu_skew_adjustment = cls.compute_skew_adjustment(curr_cpu_util)

            # Extra adjustment to handle Redshift metrics problems.
            curr_cpu_util *= gamma

            curr_cpu_denorm = curr_cpu_util * redshift_num_cpus(curr_prov)
            curr_max_cpu_denorm = curr_cpu_denorm.max().item()

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
            # Continuing to add nodes should not result in continued scaling of
            # the load beyond the max single query load.
            next_util = max(
                next_util, max_per_query_cpu_denorm / redshift_num_cpus(next_prov)
            )

            # Clip to [0, 1].
            return min(max(next_util, 0.0), 1.0)

    @classmethod
    def compute_direct_cpu_denorm(
        cls,
        query_indices: List[int],
        workload: Workload,
        next_prov: Provisioning,
        ctx: "ScoringContext",
        debug_dict: Optional[Dict[str, Any]] = None,
    ) -> Tuple[float, float]:
        if len(query_indices) == 0:
            return 0.0, 0.0

        alpha, load_max = ctx.planner_config.redshift_rt_to_cpu_denorm()
        query_run_times = workload.precomputed_redshift_analytical_latencies[next_prov][
            query_indices
        ]
        arrival_counts = workload.get_arrival_counts_batch(query_indices)
        denom = arrival_counts.sum()
        if denom > 0.0:
            arrival_weights = arrival_counts / denom
            per_query_cpu_denorm = np.clip(
                query_run_times * alpha, a_min=0.0, a_max=load_max
            )
            total_denorm = np.dot(per_query_cpu_denorm, arrival_weights).item()
            max_query_cpu_denorm = (per_query_cpu_denorm * arrival_weights).max().item()
        else:
            # Edge case: Query with 0 arrival count (used as a constraint).
            total_denorm = 0.0
            max_query_cpu_denorm = 0.0
        if debug_dict is not None:
            debug_dict["redshift_total_cpu_denorm"] = total_denorm
            debug_dict["redshift_max_query_cpu_denorm"] = max_query_cpu_denorm
        return total_denorm, max_query_cpu_denorm

    @classmethod
    def query_movement_factor(
        cls,
        query_indices: List[int],
        workload: Workload,
        ctx: "ScoringContext",
    ) -> Optional[float]:
        # Query movement scaling factor.
        # Captures change in queries routed to this engine.
        if Engine.Redshift not in ctx.engine_latency_norm_factor:
            # Special case. We cannot reweigh the queries because nothing in the
            # current workload ran on Redshift (it could have been off).
            return None
        curr_query_run_times = workload.precomputed_redshift_analytical_latencies[
            ctx.current_blueprint.redshift_provisioning()
        ][query_indices]
        norm_factor = ctx.engine_latency_norm_factor[Engine.Redshift]
        assert norm_factor != 0.0
        total_next_latency = np.dot(
            curr_query_run_times, workload.get_arrival_counts_batch(query_indices)
        )
        return total_next_latency.item() / norm_factor

    @staticmethod
    def predict_query_latency_load_resources(
        query_indices: List[int],
        workload: Workload,
        to_prov: Provisioning,
        max_node_cpu_util: float,
    ) -> npt.NDArray:
        if len(query_indices) == 0:
            return np.array([])

        # 1. Compute the impact of the provisioning.
        prov_predicted_latency = workload.precomputed_redshift_analytical_latencies[
            to_prov
        ][query_indices]

        # 2. Compute the impact of system load.
        arrival_counts = workload.get_arrival_counts_batch(query_indices)
        denom = arrival_counts.sum()
        arrival_weights = (
            arrival_counts / denom if denom > 0.0 else np.zeros_like(arrival_counts)
        )
        mean_service_time = np.dot(prov_predicted_latency, arrival_weights)
        # Note the use of p90. The predictions we make are specifically p90 latency.
        wait_time = predict_mm1_wait_time(
            mean_service_time_s=mean_service_time,
            utilization=max_node_cpu_util,
            quantile=0.9,
            alpha=1 / 8,
        )
        # Predicted running time is the query's execution time alone plus the
        # expected wait time (due to system load).
        return prov_predicted_latency + wait_time

    @classmethod
    def predict_query_latency_resources(
        cls,
        base_predicted_latency: npt.NDArray,
        to_prov: Provisioning,
        ctx: "ScoringContext",
    ) -> npt.NDArray:
        if base_predicted_latency.shape[0] == 0:
            return base_predicted_latency
        res = cls.predict_query_latency_resources_batch(
            base_predicted_latency, iter([to_prov]), ctx
        )
        return next(iter(res.values()))

    @staticmethod
    def predict_query_latency_resources_batch(
        base_predicted_latency: npt.NDArray,
        prov_id: Iterator[Provisioning],
        ctx: "ScoringContext",
    ) -> Dict[Provisioning, npt.NDArray]:
        ordering = []
        resource_factors = []
        for prov in prov_id:
            prov2 = prov.clone()
            if prov2.num_nodes() == 0:
                # Special case.
                continue
            ordering.append(prov2)
            resource_factor = _REDSHIFT_BASE_RESOURCE_VALUE / (
                redshift_num_cpus(prov2) * prov2.num_nodes()
            )
            resource_factors.append(resource_factor)

        rf = np.array(resource_factors)
        basis = np.stack([rf, np.ones_like(rf)])
        basis = np.transpose(basis)
        coefs = ctx.planner_config.redshift_new_scaling_coefs(ctx.schema_name)
        coefs = np.multiply(coefs, basis)

        num_coefs = coefs.shape[1]
        lat_vals = np.expand_dims(base_predicted_latency, axis=0)
        lat_vals = np.repeat(lat_vals, num_coefs, axis=0)
        predictions = np.matmul(coefs, lat_vals)

        assert len(ordering) == predictions.shape[0]
        assert predictions.shape[1] == base_predicted_latency.shape[0]
        return {prov: predictions[idx] for idx, prov in enumerate(ordering)}

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

    @staticmethod
    def predict_base_latency(
        latency: npt.NDArray, prov: Provisioning, ctx: "ScoringContext"
    ) -> npt.NDArray:
        if prov.num_nodes() == 0:
            return np.ones_like(latency) * np.inf
        # Ideally we should adjust for load as well.
        resource_factor = _REDSHIFT_BASE_RESOURCE_VALUE / (
            redshift_num_cpus(prov) * prov.num_nodes()
        )
        coefs = ctx.planner_config.redshift_new_scaling_coefs(ctx.schema_name)
        coefs[0] *= resource_factor
        return latency / coefs.sum()

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
