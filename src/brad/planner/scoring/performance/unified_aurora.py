import logging
import numpy as np
import numpy.typing as npt
from typing import Dict, TYPE_CHECKING, Optional, Tuple, Any, Iterator, List

from brad.config.engine import Engine
from brad.blueprint.provisioning import Provisioning
from brad.planner.scoring.provisioning import aurora_num_cpus
from brad.planner.scoring.performance.queuing import predict_mm1_wait_time
from brad.planner.workload import Workload

if TYPE_CHECKING:
    from brad.planner.scoring.context import ScoringContext

logger = logging.getLogger(__name__)


class AuroraProvisioningScore:
    def __init__(
        self,
        scaled_run_times: npt.NDArray,
        scaled_txn_lats: npt.NDArray,
        debug_values: Dict[str, int | float],
    ) -> None:
        self.scaled_run_times = scaled_run_times
        self.scaled_txn_lats = scaled_txn_lats
        self.debug_values = debug_values

    @classmethod
    def compute(
        cls,
        query_indices: List[int],
        workload: Workload,
        curr_prov: Provisioning,
        next_prov: Provisioning,
        ctx: "ScoringContext",
    ) -> "AuroraProvisioningScore":
        """
        Computes all of the Aurora provisioning-dependent scoring components in one
        place.
        """
        debug_dict: Dict[str, Any] = {}
        query_factor = cls.query_movement_factor(query_indices, workload, ctx)
        max_factor, max_factor_replace = ctx.planner_config.aurora_max_query_factor()
        if query_factor is not None and query_factor > max_factor:
            query_factor = max_factor_replace
        pred_total_cpu_denorm, max_per_query_cpu_denorm = cls.compute_direct_cpu_denorm(
            query_indices, workload, next_prov, ctx, debug_dict
        )
        has_queries = len(query_indices) > 0
        txn_cpu_denorm, ana_node_cpu_denorm = cls.predict_loads(
            has_queries,
            curr_prov,
            next_prov,
            query_factor,
            pred_total_cpu_denorm,
            max_per_query_cpu_denorm,
            ctx,
            debug_dict,
        )
        scaled_rt = cls.predict_query_latency_load_resources(
            query_indices,
            workload,
            next_prov,
            ana_node_cpu_denorm / aurora_num_cpus(next_prov),
        )
        scaled_txn_lats = cls.predict_txn_latency(
            ctx.metrics.aurora_writer_cpu_avg / 100.0 * aurora_num_cpus(curr_prov),
            txn_cpu_denorm,
            curr_prov,
            next_prov,
            ctx,
        )
        return cls(
            scaled_rt,
            scaled_txn_lats,
            {
                "aurora_query_factor": (
                    query_factor if query_factor is not None else np.nan
                ),
                "aurora_txn_cpu_denorm": txn_cpu_denorm,
                "aurora_ana_cpu_denorm": ana_node_cpu_denorm,
                **debug_dict,
            },
        )

    @classmethod
    def predict_loads(
        cls,
        has_queries: bool,
        curr_prov: Provisioning,
        next_prov: Provisioning,
        query_factor: Optional[float],
        total_cpu_denorm: float,
        max_per_query_cpu_denorm: float,
        ctx: "ScoringContext",
        debug_dict: Optional[Dict[str, Any]] = None,
    ) -> Tuple[float, float]:
        # Load is computed using the following principles:
        #
        # - Read replicas do not speed up queries, they can only "relieve" the
        #   writer node of load.
        # - Transactions must run on the writer node; only analytical queries
        #   can run on the read replica(s).
        # - If there is a read replica, BRAD routes all analytical queries to
        #   the replica.
        #
        # Output:
        # - "CPU denorm" value on `next_prov` on the writer node, for use for
        #   transaction scaling
        # - "CPU denorm" value on `next_prov` on one node that will be used for
        #   analytics scaling

        current_has_replicas = curr_prov.num_nodes() > 1
        next_has_replicas = next_prov.num_nodes() > 1

        curr_writer_cpu_util = ctx.metrics.aurora_writer_cpu_avg / 100
        curr_writer_cpu_util_denorm = curr_writer_cpu_util * aurora_num_cpus(curr_prov)

        # We take a very conservative approach to query movement. If new queries
        # are added onto Aurora, we increase the load. But if queries are
        # removed, we only decrease the load if we are using replicas. This
        # ensures we do not mix transactional and analytical load.
        query_factor_clean = 1.0
        if query_factor is not None:
            if query_factor >= 1.0:
                query_factor_clean = query_factor
            else:
                min_query_factor = ctx.planner_config.aurora_min_load_removal_fraction()
                query_factor_clean = max(min_query_factor, query_factor)

        # This is true iff we did not run any queries on Aurora with the current
        # blueprint and are now going to run queries on Aurora on the next
        # blueprint.
        adding_ana_first_time = query_factor is None and has_queries

        # 4 cases:
        # - No replicas -> No replicas
        # - No replicas -> Yes replicas
        # - Yes replicas -> No replicas
        # - Yes replicas -> Yes replicas
        if not current_has_replicas:
            # We estimate the fraction of the current writer load that belongs
            # to the transactional workload vs. the query workload.
            min_load_removal = ctx.planner_config.aurora_min_load_removal_fraction()
            pred_txn_cpu_denorm = cls.predict_txn_cpu_denorm(
                ctx.metrics.txn_completions_per_s, ctx
            )
            pred_txn_frac = pred_txn_cpu_denorm / curr_writer_cpu_util_denorm
            pred_txn_frac = max(min_load_removal, pred_txn_frac)
            pred_txn_frac = min(1.0, pred_txn_frac)
            pred_ana_frac = 1.0 - pred_txn_frac
            pred_ana_frac = max(min_load_removal, pred_ana_frac)
            pred_ana_frac = min(1.0, pred_ana_frac)
            if debug_dict is not None:
                debug_dict["aurora_pred_txn_frac"] = pred_txn_frac
                debug_dict["aurora_pred_ana_frac"] = pred_ana_frac
                debug_dict["aurora_pred_txn_cpu_denorm"] = pred_txn_cpu_denorm

            if not next_has_replicas:
                # No replicas -> No replicas
                if adding_ana_first_time:
                    # Special case. If no queries ran on Aurora and now we are
                    # running queries, we use the predicted total load.
                    return (
                        curr_writer_cpu_util_denorm + total_cpu_denorm,
                        curr_writer_cpu_util_denorm + total_cpu_denorm,
                    )
                else:
                    final_denorm = (
                        curr_writer_cpu_util_denorm * pred_txn_frac
                        + curr_writer_cpu_util_denorm
                        * (1.0 - pred_txn_frac)
                        * query_factor_clean
                    )
                    return (final_denorm, final_denorm)
            else:
                # No replicas -> Yes replicas
                if adding_ana_first_time:
                    # Special case. If no queries ran on Aurora and now we are
                    # running queries, we use the predicted total load.
                    return (
                        curr_writer_cpu_util_denorm,
                        max(
                            total_cpu_denorm / (next_prov.num_nodes() - 1),
                            # Cannot scale below this value (single query).
                            max_per_query_cpu_denorm,
                        ),
                    )
                else:
                    # Here we need to separate the load imposed by the
                    # transactions and the load imposed by the other queries.
                    # Transactions remain on Aurora.
                    return (
                        curr_writer_cpu_util_denorm * pred_txn_frac,
                        max(
                            (
                                curr_writer_cpu_util_denorm
                                * pred_ana_frac
                                * query_factor_clean
                            )
                            / (next_prov.num_nodes() - 1),
                            # Cannot scale below this value (single query).
                            max_per_query_cpu_denorm,
                        ),
                    )

        else:
            # We currently have read replicas.
            curr_num_read_replicas = curr_prov.num_nodes() - 1
            total_reader_cpu_denorm = (
                (ctx.metrics.aurora_reader_cpu_avg / 100)
                * aurora_num_cpus(curr_prov)
                * curr_num_read_replicas
            )

            if not next_has_replicas:
                # Yes replicas -> No replicas
                if adding_ana_first_time:
                    # Special case.
                    return (
                        curr_writer_cpu_util_denorm + total_cpu_denorm,
                        (curr_writer_cpu_util_denorm + total_cpu_denorm),
                    )
                else:
                    return (
                        curr_writer_cpu_util_denorm
                        + (total_reader_cpu_denorm * query_factor_clean),
                        curr_writer_cpu_util_denorm
                        + (total_reader_cpu_denorm * query_factor_clean),
                    )

            else:
                # Yes replicas -> Yes replicas
                if adding_ana_first_time:
                    # Special case.
                    return (
                        curr_writer_cpu_util_denorm,
                        max(
                            total_cpu_denorm / (next_prov.num_nodes() - 1),
                            # Cannot scale below this value (single query).
                            max_per_query_cpu_denorm,
                        ),
                    )
                else:
                    return (
                        curr_writer_cpu_util_denorm,
                        max(
                            total_reader_cpu_denorm
                            * query_factor_clean
                            / (next_prov.num_nodes() - 1),
                            # Cannot scale below this value (single query).
                            max_per_query_cpu_denorm,
                        ),
                    )

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

        alpha, load_max = ctx.planner_config.aurora_rt_to_cpu_denorm()
        query_run_times = workload.precomputed_aurora_analytical_latencies[next_prov][
            query_indices
        ]
        arrival_counts = workload.get_arrival_counts_batch(query_indices)
        denom = arrival_counts.sum()
        if denom > 0.0:
            arrival_weights = arrival_counts / denom
            per_query_cpu_denorm = np.clip(
                query_run_times * alpha, a_min=0.0, a_max=load_max
            )
            total_denorm = np.dot(per_query_cpu_denorm, arrival_weights)
            max_query_cpu_denorm = per_query_cpu_denorm.max()
        else:
            # Edge case: Query with 0 arrival count (used as a constraint).
            total_denorm = np.zeros_like(query_run_times)
            max_query_cpu_denorm = 0.0
        if debug_dict is not None:
            debug_dict["aurora_total_cpu_denorm"] = total_denorm
            debug_dict["aurora_max_query_cpu_denorm"] = max_query_cpu_denorm
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
        if Engine.Aurora not in ctx.engine_latency_norm_factor:
            # Special case. We cannot reweigh the queries because nothing in the
            # current workload ran on Aurora.
            return None
        curr_query_run_times = workload.precomputed_aurora_analytical_latencies[
            ctx.current_blueprint.aurora_provisioning()
        ][query_indices]
        norm_factor = ctx.engine_latency_norm_factor[Engine.Aurora]
        assert norm_factor != 0.0
        total_next_latency = np.dot(
            curr_query_run_times, workload.get_arrival_counts_batch(query_indices)
        )
        return total_next_latency / norm_factor

    @classmethod
    def predict_query_latency_load_resources(
        cls,
        query_indices: List[int],
        workload: Workload,
        to_prov: Provisioning,
        cpu_util: float,
    ) -> npt.NDArray:
        if len(query_indices) == 0:
            return np.array([])

        prov_predicted_latency = workload.precomputed_aurora_analytical_latencies[
            to_prov
        ][query_indices]

        arrival_counts = workload.get_arrival_counts_batch(query_indices)
        denom = arrival_counts.sum()
        arrival_weights = (
            arrival_counts / denom if denom > 0.0 else np.zeros_like(arrival_counts)
        )
        mean_service_time = np.dot(prov_predicted_latency, arrival_weights)
        # Note the use of p90. The predictions we make are specifically p90 latency.
        wait_time = predict_mm1_wait_time(
            mean_service_time_s=mean_service_time,
            utilization=cpu_util,
            quantile=0.9,
            alpha=1 / 8,
        )
        # Predicted running time is the query's execution time alone plus the
        # expected wait time (due to system load)
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
        prov_it: Iterator[Provisioning],
        ctx: "ScoringContext",
    ) -> Dict[Provisioning, npt.NDArray]:
        ordering = []
        resource_factors = []
        for prov in prov_it:
            prov2 = prov.clone()
            ordering.append(prov2)
            resource_factor = _AURORA_BASE_RESOURCE_VALUE / aurora_num_cpus(prov2)
            resource_factors.append(resource_factor)

        rf = np.array(resource_factors)
        basis = np.stack([rf, np.ones_like(rf)])
        basis = np.transpose(basis)
        coefs = ctx.planner_config.aurora_new_scaling_coefs()
        coefs = np.multiply(coefs, basis)

        num_coefs = coefs.shape[1]
        lat_vals = np.expand_dims(base_predicted_latency, axis=0)
        lat_vals = np.repeat(lat_vals, num_coefs, axis=0)

        predictions = np.matmul(coefs, lat_vals)

        assert len(ordering) == predictions.shape[0]
        assert predictions.shape[1] == base_predicted_latency.shape[0]
        return {prov: predictions[idx] for idx, prov in enumerate(ordering)}

    @staticmethod
    def predict_query_latency_load_resources_legacy(
        base_predicted_latency: npt.NDArray,
        to_prov: Provisioning,
        overall_load: float,
        ctx: "ScoringContext",
    ) -> npt.NDArray:
        # Special case:
        if (
            overall_load == 0.0
            and aurora_num_cpus(to_prov) == _AURORA_BASE_RESOURCE_VALUE
        ):
            # No changes.
            return base_predicted_latency

        # This method is used to compute the predicted query latencies.
        resource_factor = _AURORA_BASE_RESOURCE_VALUE / aurora_num_cpus(to_prov)
        basis = np.array(
            [overall_load * resource_factor, overall_load, resource_factor, 1.0]
        )
        basis = np.square(basis)
        coefs = ctx.planner_config.aurora_scaling_coefs()
        coefs = np.multiply(coefs, basis)
        num_coefs = coefs.shape[0]

        lat_vals = np.expand_dims(base_predicted_latency, axis=1)
        lat_vals = np.repeat(lat_vals, num_coefs, axis=1)

        return np.dot(lat_vals, coefs)

    @staticmethod
    def predict_txn_latency(
        curr_cpu_denorm: float,
        next_cpu_denorm: float,
        curr_prov: Provisioning,
        to_prov: Provisioning,
        ctx: "ScoringContext",
    ) -> npt.NDArray:
        observed_lats = np.array([ctx.metrics.txn_lat_s_p50, ctx.metrics.txn_lat_s_p90])

        # Q(u) = a / (K - u) + b ; u is CPU utilization in [0, 1]
        # --> Q(u') = (K - u) / (K - u') (Q(u) - b) + b

        model = ctx.planner_config.aurora_txn_coefs(ctx.schema_name)
        K = model["K"]
        b = np.array([model["b_p50"], model["b_p90"]])

        curr_num_cpus = aurora_num_cpus(curr_prov)
        next_num_cpus = aurora_num_cpus(to_prov)
        curr_cpu_util = min(curr_cpu_denorm / curr_num_cpus, 1.0)
        next_cpu_util = min(next_cpu_denorm / next_num_cpus, 1.0)

        # To avoid division by zero in degenerate cases.
        denom = max(K - next_cpu_util, 1e-6)
        sf = (K - curr_cpu_util) / denom

        without_base = np.clip(observed_lats - b, a_min=0.0, a_max=None)
        pred_dest = (without_base * sf) + b

        # If the observed latencies were not defined, we should not make a prediction.
        pred_dest[observed_lats == 0.0] = np.nan
        pred_dest[~np.isfinite(observed_lats)] = np.nan

        return pred_dest

    @staticmethod
    def predict_txn_cpu_denorm(completions_per_s: float, ctx: "ScoringContext"):
        model = ctx.planner_config.aurora_txn_coefs(ctx.schema_name)
        return completions_per_s * model["C_1"]

    @staticmethod
    def predict_base_latency(
        latency: npt.NDArray, prov: Provisioning, ctx: "ScoringContext"
    ) -> npt.NDArray:
        if prov.num_nodes() == 0:
            return np.ones_like(latency) * np.inf
        # Ideally we should adjust for load as well.
        resource_factor = _AURORA_BASE_RESOURCE_VALUE / aurora_num_cpus(prov)
        coefs = ctx.planner_config.aurora_new_scaling_coefs()
        coefs[0] *= resource_factor
        return latency / coefs.sum()

    def copy(self) -> "AuroraProvisioningScore":
        return AuroraProvisioningScore(
            self.scaled_run_times,
            self.scaled_txn_lats,
            self.debug_values.copy(),
        )

    def add_debug_values(self, dest: Dict[str, int | float | str]) -> None:
        """
        Adds this score instance's debug values to the `dest` dict.
        """
        (
            dest["aurora_pred_txn_lat_s_p50"],
            dest["aurora_pred_txn_lat_s_p90"],
        ) = self.scaled_txn_lats
        dest.update(self.debug_values)


_AURORA_BASE_RESOURCE_VALUE = aurora_num_cpus(Provisioning("db.r6g.xlarge", 1))
