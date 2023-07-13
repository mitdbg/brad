import logging
import numpy as np
import numpy.typing as npt
from typing import Dict

from brad.config.engine import Engine
from brad.blueprint.provisioning import Provisioning
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.provisioning import aurora_resource_value

logger = logging.getLogger(__name__)


class AuroraProvisioningScore:
    def __init__(
        self,
        scaled_run_times: npt.NDArray,
        overall_system_load: float,
        pred_txn_peak_load: float,
        for_next_prov: Provisioning,
        debug_values: Dict[str, int | float],
    ) -> None:
        self.scaled_run_times = scaled_run_times
        self.debug_values = debug_values
        self.overall_system_load = overall_system_load
        self.pred_txn_peak_load = pred_txn_peak_load
        self.for_next_prov = for_next_prov

    @classmethod
    def compute(
        cls,
        base_query_run_times: npt.NDArray,
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

        # 1. Compute the transaction portion of load.
        client_txns_per_s = ctx.metrics.client_txn_completions_per_s_avg
        coef, intercept = ctx.planner_config.client_txn_thpt_model()
        pred_txn_load = coef * client_txns_per_s + intercept

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

        # 2. Adjust the analytical portion of the system load for query movement.
        if Engine.Aurora not in ctx.current_latency_weights:
            # Special case. We cannot reweigh the queries because nothing in the
            # current workload ran on Aurora.
            query_factor = 1.0
        else:
            # We scale the predicted query execution times by a factor "l", which is
            # meant to capture the load on the system (e.g., concurrently running
            # queries). We model l as being proportional to the predicted change in CPU
            # utilization across deployments.
            #
            # First we calculate the predicted CPU utilization on the next blueprint
            # (Redshift provisioning and query placement). Then we compute the CPU
            # utilization change and translate this value into l.

            # Query movement scaling factor.
            # Captures change in queries routed to this engine.
            base_latency = ctx.current_latency_weights[Engine.Aurora]
            assert base_latency != 0.0
            total_next_latency = base_query_run_times.sum()
            query_factor = total_next_latency / base_latency
        analytics_load = max(0, overall_forecasted_load - pred_txn_load)
        analytics_load *= query_factor

        # 3. Combine the load factors again.
        adjusted_overall_load = analytics_load + pred_txn_load

        # 4. Scale query execution times based on load and provisioning.
        # TODO: Replace with our unified model.
        prov_scaled = cls._scale_aurora_predicted_latency(
            base_query_run_times, next_prov, ctx
        )
        load_scaled = cls._scale_aurora_run_time_by_load(
            prov_scaled, adjusted_overall_load, ctx
        )

        return cls(
            load_scaled,
            adjusted_overall_load,
            np.inf,
            next_prov,
            {
                "pred_txn_load": pred_txn_load,
                "query_factor": query_factor,
                "adjusted_analytics_load": analytics_load,
            },
        )

    ###
    ### The methods below are our "legacy" scaling methods. They will be
    ### adjusted in a future commit.
    ###

    @staticmethod
    def _scale_aurora_predicted_latency(
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

    @staticmethod
    def _scale_aurora_run_time_by_load(
        base_run_times: npt.NDArray, next_load: float, ctx: ScoringContext
    ) -> npt.NDArray:
        """
        `next_load` is a unit-less number that should be above 0.
        """
        return base_run_times * ctx.planner_config.aurora_load_alpha() * next_load


_AURORA_BASE_RESOURCE_VALUE = aurora_resource_value(Provisioning("db.r6g.large", 1))
