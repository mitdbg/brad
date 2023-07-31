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
        overall_system_load: float,
        overall_cpu_denorm: float,
        pred_txn_peak_cpu_denorm: float,
        for_next_prov: Provisioning,
        debug_values: Dict[str, int | float],
    ) -> None:
        self.scaled_run_times = scaled_run_times
        self.debug_values = debug_values
        self.overall_system_load = overall_system_load
        self.overall_cpu_denorm = overall_cpu_denorm
        self.pred_txn_peak_cpu_denorm = pred_txn_peak_cpu_denorm
        self.for_next_prov = for_next_prov

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
        # TODO: Flesh out the changes needed for read replicas.
        # - Read replicas do not speed up queries, they can only "relieve" the
        #   writer node of load.
        # - Transactions must run on the writer node; only analytical queries
        #   can run on the read replica(s).
        # - We may need a read replica routing policy. For example, if one
        #   exists, do *all* analytical queries go to the read replica(s) by
        #   default?
        overall_forecasted_load = ctx.metrics.aurora_load_minute_avg
        overall_forecasted_cpu_util_pct = ctx.metrics.aurora_cpu_avg
        overall_cpu_util_denorm = (
            overall_forecasted_cpu_util_pct / 100
        ) * aurora_num_cpus(curr_prov)

        # 1. Compute the transaction portion of load.
        client_txns_per_s = ctx.metrics.txn_completions_per_s
        pred_txn_load = client_txns_per_s * ctx.planner_config.client_txn_to_load()
        pred_txn_cpu_denorm = (
            client_txns_per_s * ctx.planner_config.client_txn_to_cpu_denorm()
        )

        # TODO: Possible edge cases here due to cumulative prediction error (e.g.,
        # pred_txn_load > overall_forecasted_load violates our model's assumptions).
        # We need a robust way to handle these potential errors.
        if pred_txn_load > overall_forecasted_load:
            logger.warning(
                "Predicted transactional load higher than the overall forecasted load. "
                "Overall load: %.2f, Client txn thpt: %.2f, Predicted txn load: %.2f",
                overall_forecasted_load,
                client_txns_per_s,
                pred_txn_load,
            )
        if pred_txn_cpu_denorm > overall_cpu_util_denorm:
            logger.warning(
                "Predicted transactional CPU denormalized utilization higher than the overall "
                "forecasted CPU use. Overall use: %.2f, Client txn thpt: %.2f, Predicted CPU "
                "use: %.2f",
                overall_cpu_util_denorm,
                client_txns_per_s,
                pred_txn_cpu_denorm,
            )

        # 2. Adjust the analytical portion of the system load for query movement.
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

        analytics_load = max(0, overall_forecasted_load - pred_txn_load)
        analytics_load *= query_factor

        analytics_cpu_denorm = max(0, overall_cpu_util_denorm - pred_txn_cpu_denorm)
        analytics_cpu_denorm *= query_factor

        # 3. Combine the load factors again.
        adjusted_overall_load = analytics_load + pred_txn_load
        adjusted_overall_cpu_denorm = analytics_cpu_denorm + pred_txn_cpu_denorm

        # 4. Scale query execution times based on load and provisioning.
        scaled_rt = cls._scale_load_resources(
            base_query_run_times, next_prov, adjusted_overall_load, ctx
        )

        # 5. Compute the expected peak CPU.
        peak_cpu_denorm = (
            aurora_num_cpus(next_prov)
            * ctx.planner_config.aurora_prov_to_peak_cpu_denorm()
        )

        return cls(
            scaled_rt,
            adjusted_overall_load,
            adjusted_overall_cpu_denorm,
            peak_cpu_denorm,
            next_prov,
            {
                "pred_txn_load": pred_txn_load,
                "pred_txn_cpu_denorm": pred_txn_cpu_denorm,
                "query_factor": query_factor,
                "adjusted_analytics_load": analytics_load,
                "adjusted_analytics_cpu_denorm": analytics_cpu_denorm,
            },
        )

    @staticmethod
    def _scale_load_resources(
        base_predicted_latency: npt.NDArray,
        to_prov: Provisioning,
        overall_load: float,
        ctx: ScoringContext,
    ) -> npt.NDArray:
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

    def copy(self) -> "AuroraProvisioningScore":
        return AuroraProvisioningScore(
            self.scaled_run_times,
            self.overall_system_load,
            self.overall_cpu_denorm,
            self.pred_txn_peak_cpu_denorm,
            self.for_next_prov,
            self.debug_values.copy(),
        )


_AURORA_BASE_RESOURCE_VALUE = aurora_num_cpus(Provisioning("db.r6g.large", 1))
