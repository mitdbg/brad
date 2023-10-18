import logging
import numpy as np
import numpy.typing as npt
from typing import Dict

from brad.config.engine import Engine
from brad.blueprint.provisioning import Provisioning
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.provisioning import aurora_num_cpus

logger = logging.getLogger(__name__)


class AuroraProvisioningScore:
    def __init__(
        self,
        scaled_run_times: npt.NDArray,
        scaled_txn_lats: npt.NDArray,
        analytics_affected_load: float,
        analytics_affected_cpu_denorm: float,
        txn_affected_cpu_denorm: float,
        pred_txn_peak_cpu_denorm: float,
        for_next_prov: Provisioning,
        debug_values: Dict[str, int | float],
    ) -> None:
        self.scaled_run_times = scaled_run_times
        self.debug_values = debug_values
        self.analytics_affected_load = analytics_affected_load
        self.analytics_affected_cpu_denorm = analytics_affected_cpu_denorm
        self.txn_affected_cpu_denorm = txn_affected_cpu_denorm
        self.pred_txn_peak_cpu_denorm = pred_txn_peak_cpu_denorm
        self.for_next_prov = for_next_prov
        self.scaled_txn_lats = scaled_txn_lats

    @classmethod
    def compute(
        cls,
        base_query_run_times: npt.NDArray,
        curr_prov: Provisioning,
        next_prov: Provisioning,
        ctx: ScoringContext,
    ) -> "AuroraProvisioningScore":
        """
        Computes all of the Aurora provisioning-dependent scoring components in one
        place.
        """
        # - Read replicas do not speed up queries, they can only "relieve" the
        #   writer node of load.
        # - Transactions must run on the writer node; only analytical queries
        #   can run on the read replica(s).
        # - If there is a read replica, BRAD routes all analytical queries to
        #   the replica.

        current_aurora_has_replicas = curr_prov.num_nodes() > 1
        next_aurora_has_replicas = next_prov.num_nodes() > 1

        overall_writer_load = ctx.metrics.aurora_writer_load_minute_avg
        overall_writer_cpu_util_pct = ctx.metrics.aurora_writer_cpu_avg
        overall_writer_cpu_util = overall_writer_cpu_util_pct / 100
        overall_writer_cpu_util_denorm = overall_writer_cpu_util * aurora_num_cpus(
            curr_prov
        )

        # 1. Compute the transaction portion of load.
        if current_aurora_has_replicas:
            # We schedule all analytics on the read replica(s). So the metric
            # values on the writer are due to the transactional workload.
            pred_txn_load = overall_writer_load
            pred_txn_cpu_denorm = overall_writer_cpu_util_denorm
        else:
            client_txns_per_s = ctx.metrics.txn_completions_per_s
            pred_txn_load = client_txns_per_s * ctx.planner_config.client_txn_to_load()
            pred_txn_cpu_denorm = (
                client_txns_per_s * ctx.planner_config.client_txn_to_cpu_denorm()
            )

            # TODO: Possible edge cases here due to cumulative prediction error (e.g.,
            # pred_txn_load > overall_writer_load violates our model's assumptions).
            # We need a robust way to handle these potential errors.
            if pred_txn_load > overall_writer_load:
                logger.warning(
                    "Predicted transactional load higher than the overall writer load. "
                    "Overall load: %.2f, Client txn thpt: %.2f, Predicted txn load: %.2f",
                    overall_writer_load,
                    client_txns_per_s,
                    pred_txn_load,
                )
            if pred_txn_cpu_denorm > overall_writer_cpu_util_denorm:
                logger.warning(
                    "Predicted transactional CPU denormalized utilization higher than the overall "
                    "CPU use. Overall use: %.2f, Client txn thpt: %.2f, Predicted CPU "
                    "use: %.2f",
                    overall_writer_cpu_util_denorm,
                    client_txns_per_s,
                    pred_txn_cpu_denorm,
                )

        # 2. Adjust the analytical portion of the system load for query movement
        #    (compute `query_factor``).
        if Engine.Aurora not in ctx.current_latency_weights:
            # Special case. We cannot reweigh the queries because nothing in the
            # current workload ran on Aurora.
            query_factor = 1.0
        else:
            # Query movement scaling factor.
            # Captures change in queries routed to this engine.
            base_latency = ctx.current_latency_weights[Engine.Aurora]
            assert base_latency != 0.0
            total_next_latency = base_query_run_times.sum()
            query_factor = total_next_latency / base_latency

        # 3. Compute the analytics portion of the load and adjust it by the query factor.
        if current_aurora_has_replicas:
            num_read_replicas = curr_prov.num_nodes() - 1
            analytics_load = (
                ctx.metrics.aurora_reader_load_minute_avg * num_read_replicas
            )
            analytics_cpu_denorm = (
                (ctx.metrics.aurora_reader_cpu_avg / 100)
                * aurora_num_cpus(curr_prov)
                * num_read_replicas
            )
        else:
            analytics_load = max(0, overall_writer_load - pred_txn_load)
            analytics_cpu_denorm = max(
                0, overall_writer_cpu_util_denorm - pred_txn_cpu_denorm
            )
        analytics_load *= query_factor
        analytics_cpu_denorm *= query_factor

        # 4. Compute the workload-affected metrics.
        # Basically, if there are no replicas, both the analytical and
        # transactional load fall onto one instance (which we need to capture).
        if next_aurora_has_replicas:
            analytics_affected_load = analytics_load
            analytics_affected_cpu_denorm = analytics_cpu_denorm
            txn_affected_cpu_denorm = pred_txn_cpu_denorm
        else:
            analytics_affected_load = analytics_load + pred_txn_load
            analytics_affected_cpu_denorm = analytics_cpu_denorm + pred_txn_cpu_denorm
            txn_affected_cpu_denorm = analytics_affected_cpu_denorm

        # 4. Predict query execution times based on load and provisioning.
        scaled_rt = cls._query_latency_load_resources(
            base_query_run_times, next_prov, analytics_affected_load, ctx
        )

        # 5. Compute the expected peak CPU.
        peak_cpu_denorm = (
            aurora_num_cpus(next_prov)
            * ctx.planner_config.aurora_prov_to_peak_cpu_denorm()
        )

        # 6. Compute the transactional latencies.
        scaled_txn_lats = cls._scale_txn_latency(
            overall_writer_cpu_util_denorm,
            txn_affected_cpu_denorm,
            curr_prov,
            next_prov,
            ctx,
        )

        return cls(
            scaled_rt,
            scaled_txn_lats,
            analytics_affected_load,
            analytics_affected_cpu_denorm,
            txn_affected_cpu_denorm,
            peak_cpu_denorm,
            next_prov,
            {
                "aurora_pred_txn_load": pred_txn_load,
                "aurora_pred_txn_cpu_denorm": pred_txn_cpu_denorm,
                "aurora_query_factor": query_factor,
                "aurora_internal_analytics_load": analytics_load,
                "aurora_internal_analytics_cpu_denorm": analytics_cpu_denorm,
            },
        )

    @staticmethod
    def _query_latency_load_resources(
        base_predicted_latency: npt.NDArray,
        to_prov: Provisioning,
        overall_load: float,
        ctx: ScoringContext,
    ) -> npt.NDArray:
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
    def _scale_txn_latency(
        curr_cpu_denorm: float,
        next_cpu_denorm: float,
        curr_prov: Provisioning,
        to_prov: Provisioning,
        ctx: ScoringContext,
    ) -> npt.NDArray:
        observed_lats = np.array([ctx.metrics.txn_lat_s_p50, ctx.metrics.txn_lat_s_p90])

        # Q(u, r_c, r_d) = a (K_l K_r) / (K_l r_d - u r_c) + b
        # We compute (a (K_l K_r)) based on the current observations.
        # Then use this value to predict the run time based on the load and resource differences.
        model = ctx.planner_config.aurora_txn_coefs(ctx.schema_name)
        K_l = model["K_l"]
        b = np.array([model["b_p50"], model["b_p90"]])

        curr_num_cpus = aurora_num_cpus(curr_prov)
        curr_cpu_util = curr_cpu_denorm / curr_num_cpus
        coef_base = 1.0 / ((K_l - curr_cpu_util) * curr_num_cpus)
        base_wo_b = observed_lats - b
        eps = b * 0.01
        base_wo_b = np.maximum(base_wo_b, eps)  # Used to avoid degenerate cases.
        comp_const = base_wo_b / coef_base

        dest_num_cpus = aurora_num_cpus(to_prov)
        dest_cpu_util = next_cpu_denorm / dest_num_cpus
        dest_cpu_util = min(dest_cpu_util, K_l - 0.01)  # To avoid degenerate cases.
        coef_dest = 1.0 / ((K_l - dest_cpu_util) * dest_num_cpus)
        pred_dest = b + (comp_const * coef_dest)

        # If the observed latencies were not defined, we should not make a prediction.
        pred_dest[observed_lats == 0.0] = np.nan
        pred_dest[~np.isfinite(observed_lats)] = np.nan

        return pred_dest

    def copy(self) -> "AuroraProvisioningScore":
        return AuroraProvisioningScore(
            self.scaled_run_times,
            self.scaled_txn_lats,
            self.analytics_affected_load,
            self.analytics_affected_cpu_denorm,
            self.txn_affected_cpu_denorm,
            self.pred_txn_peak_cpu_denorm,
            self.for_next_prov,
            self.debug_values.copy(),
        )

    def add_debug_values(self, dest: Dict[str, int | float | str]) -> None:
        """
        Adds this score instance's debug values to the `dest` dict.
        """
        dest["aurora_analytics_affected_load"] = self.analytics_affected_load
        dest[
            "aurora_analytics_affected_cpu_denorm"
        ] = self.analytics_affected_cpu_denorm
        dest["aurora_txn_affected_cpu_denorm"] = self.txn_affected_cpu_denorm
        dest["aurora_pred_txn_peak_cpu_denorm"] = self.pred_txn_peak_cpu_denorm
        (
            dest["aurora_pred_txn_lat_s_p50"],
            dest["aurora_pred_txn_lat_s_p90"],
        ) = self.scaled_txn_lats
        dest.update(self.debug_values)


_AURORA_BASE_RESOURCE_VALUE = aurora_num_cpus(Provisioning("db.r6g.large", 1))
