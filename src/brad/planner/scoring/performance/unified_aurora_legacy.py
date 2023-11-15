import logging
import numpy as np
import numpy.typing as npt
from typing import Dict, TYPE_CHECKING

from brad.config.engine import Engine
from brad.blueprint.provisioning import Provisioning
from brad.planner.scoring.provisioning import aurora_num_cpus

if TYPE_CHECKING:
    from brad.planner.scoring.context import ScoringContext

logger = logging.getLogger(__name__)


class AuroraProvisioningScoreLegacy:
    def __init__(
        self,
        scaled_run_times: npt.NDArray,
        scaled_txn_lats: npt.NDArray,
        analytics_affected_per_machine_load: float,
        analytics_affected_per_machine_cpu_denorm: float,
        txn_affected_cpu_denorm: float,
        pred_txn_peak_cpu_denorm: float,
        for_next_prov: Provisioning,
        debug_values: Dict[str, int | float],
    ) -> None:
        self.scaled_run_times = scaled_run_times
        self.debug_values = debug_values
        self.analytics_affected_per_machine_load = analytics_affected_per_machine_load
        self.analytics_affected_per_machine_cpu_denorm = (
            analytics_affected_per_machine_cpu_denorm
        )
        self.txn_affected_cpu_denorm = txn_affected_cpu_denorm
        self.pred_txn_peak_cpu_denorm = pred_txn_peak_cpu_denorm
        self.for_next_prov = for_next_prov
        self.scaled_txn_lats = scaled_txn_lats

    @classmethod
    def compute(
        cls,
        base_query_run_times: npt.NDArray,
        query_arrival_counts: npt.NDArray,
        curr_prov: Provisioning,
        next_prov: Provisioning,
        ctx: "ScoringContext",
    ) -> "AuroraProvisioningScoreLegacy":
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
        no_analytics_queries_executed = (
            len(ctx.current_query_locations[Engine.Aurora]) == 0
        )

        overall_writer_load = ctx.metrics.aurora_writer_load_minute_avg
        overall_writer_cpu_util_pct = ctx.metrics.aurora_writer_cpu_avg
        overall_writer_cpu_util = overall_writer_cpu_util_pct / 100
        overall_writer_cpu_util_denorm = overall_writer_cpu_util * aurora_num_cpus(
            curr_prov
        )

        # 1. Compute the transaction portion of load.
        if current_aurora_has_replicas or no_analytics_queries_executed:
            # We schedule all analytics on the read replica(s) or no queries
            # were routed to Aurora. So the metric values on the writer are due
            # to the transactional workload.
            pred_txn_load = overall_writer_load
            pred_txn_cpu_denorm = overall_writer_cpu_util_denorm
        else:
            model = ctx.planner_config.aurora_txn_coefs(ctx.schema_name)
            curr_num_cpus = aurora_num_cpus(curr_prov)
            client_txns_per_s = ctx.metrics.txn_completions_per_s

            # Piecewise function; the inflection point appears due to (maybe)
            # hyperthreading behavior.
            #
            # D(client_txns_per_s, curr_cpus) =
            #   C_1 * client_txns_per_s    if C_1 * client_txns_per_s <= curr_cpus / 2
            #   C_2 * (client_txns_per_s - curr_cpus / (2 * C_1)) + curr_cpus / 2    otherwise
            cpu_denorm_limit = curr_num_cpus / 2
            pred_txn_cpu_denorm = client_txns_per_s * model["C_1"]
            if pred_txn_cpu_denorm > cpu_denorm_limit:
                pred_txn_cpu_denorm = (
                    model["C_2"] * (client_txns_per_s - cpu_denorm_limit / model["C_1"])
                    + cpu_denorm_limit
                )

            # In theory, these two should be equal. Empirically, they are mostly close enough.
            pred_txn_load = pred_txn_cpu_denorm

            # TODO: Possible edge cases here due to cumulative prediction error (e.g.,
            # pred_txn_load > overall_writer_load violates our model's assumptions).
            # We need a robust way to handle these potential errors.
            if not ctx.already_logged_txn_interference_warning:
                if pred_txn_load > overall_writer_load:
                    logger.warning(
                        "Predicted transactional load higher than the overall "
                        "writer load. Overall load: %.2f, Client txn thpt: %.2f, "
                        "Predicted txn load: %.2f",
                        overall_writer_load,
                        client_txns_per_s,
                        pred_txn_load,
                    )
                    ctx.already_logged_txn_interference_warning = True

                if pred_txn_cpu_denorm > overall_writer_cpu_util_denorm:
                    logger.warning(
                        "Predicted transactional CPU denormalized utilization "
                        "higher than the overall CPU use. Overall use: %.2f, "
                        "Client txn thpt: %.2f, Predicted CPU use: %.2f",
                        overall_writer_cpu_util_denorm,
                        client_txns_per_s,
                        pred_txn_cpu_denorm,
                    )
                    ctx.already_logged_txn_interference_warning = True

        # 2. Adjust the analytical portion of the system load for query movement
        #    (compute `query_factor``).
        if Engine.Aurora not in ctx.engine_latency_norm_factor:
            # Special case. We cannot reweigh the queries because nothing in the
            # current workload ran on Aurora.
            query_factor = 1.0
        else:
            # Query movement scaling factor.
            # Captures change in queries routed to this engine.
            norm_factor = ctx.engine_latency_norm_factor[Engine.Aurora]
            assert norm_factor != 0.0
            total_next_latency = np.dot(base_query_run_times, query_arrival_counts)
            query_factor = total_next_latency / norm_factor

        # 3. Compute the analytics portion of the load and adjust it by the query factor.
        if current_aurora_has_replicas:
            curr_num_read_replicas = curr_prov.num_nodes() - 1
            total_analytics_load = (
                ctx.metrics.aurora_reader_load_minute_avg * curr_num_read_replicas
            )
            total_analytics_cpu_denorm = (
                (ctx.metrics.aurora_reader_cpu_avg / 100)
                * aurora_num_cpus(curr_prov)
                * curr_num_read_replicas
            )

        elif no_analytics_queries_executed:
            # This is a special case: no queries executed on Aurora and there
            # was no read replica.
            total_analytics_load = 0.0
            total_analytics_cpu_denorm = 0.0

        else:
            # Analytics load should never be zero - so we impute a small value
            # to work around mispredictions.
            eps = 1e-3 if len(base_query_run_times) > 0 else 0.0
            total_analytics_load = max(eps, overall_writer_load - pred_txn_load)
            total_analytics_cpu_denorm = max(
                eps, overall_writer_cpu_util_denorm - pred_txn_cpu_denorm
            )

        # total_analytics_load *= query_factor
        # total_analytics_cpu_denorm *= query_factor

        # 4. Compute the workload-affected metrics.
        # Basically, if there are no replicas, both the analytical and
        # transactional load fall onto one instance (which we need to capture).
        if next_aurora_has_replicas:
            next_num_read_replicas = next_prov.num_nodes() - 1
            assert next_num_read_replicas > 0

            if no_analytics_queries_executed and len(base_query_run_times) > 0:
                # We need to use a non-zero load. We use a constant factor to
                # prime the system.
                total_analytics_load = (
                    ctx.planner_config.aurora_initialize_load_fraction()
                    * aurora_num_cpus(ctx.current_blueprint.aurora_provisioning())
                    * ctx.current_blueprint.aurora_provisioning().num_nodes()
                )
                total_analytics_cpu_denorm = total_analytics_load

            # Divide by the number of read replicas: we assume the load can
            # be equally divided amongst the replicas.
            analytics_affected_per_machine_load = (
                total_analytics_load / next_num_read_replicas
            )
            analytics_affected_per_machine_cpu_denorm = (
                total_analytics_cpu_denorm / next_num_read_replicas
            )
            txn_affected_cpu_denorm = pred_txn_cpu_denorm
        else:
            analytics_affected_per_machine_load = total_analytics_load + pred_txn_load
            analytics_affected_per_machine_cpu_denorm = (
                total_analytics_cpu_denorm + pred_txn_cpu_denorm
            )
            # Basically the same as above, because they are running together.
            txn_affected_cpu_denorm = analytics_affected_per_machine_cpu_denorm

        # 4. Predict query execution times based on load and provisioning.
        scaled_rt = cls.query_latency_load_resources(
            base_query_run_times, next_prov, analytics_affected_per_machine_load, ctx
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
            analytics_affected_per_machine_load,
            analytics_affected_per_machine_cpu_denorm,
            txn_affected_cpu_denorm,
            peak_cpu_denorm,
            next_prov,
            {
                "aurora_pred_txn_load": pred_txn_load,
                "aurora_pred_txn_cpu_denorm": pred_txn_cpu_denorm,
                "aurora_query_factor": query_factor,
                "aurora_internal_total_analytics_load": total_analytics_load,
                "aurora_internal_total_analytics_cpu_denorm": total_analytics_cpu_denorm,
            },
        )

    @staticmethod
    def query_latency_load_resources(
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
    def _scale_txn_latency(
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

    def copy(self) -> "AuroraProvisioningScoreLegacy":
        return AuroraProvisioningScoreLegacy(
            self.scaled_run_times,
            self.scaled_txn_lats,
            self.analytics_affected_per_machine_load,
            self.analytics_affected_per_machine_cpu_denorm,
            self.txn_affected_cpu_denorm,
            self.pred_txn_peak_cpu_denorm,
            self.for_next_prov,
            self.debug_values.copy(),
        )

    def add_debug_values(self, dest: Dict[str, int | float | str]) -> None:
        """
        Adds this score instance's debug values to the `dest` dict.
        """
        dest[
            "aurora_analytics_affected_per_machine_load"
        ] = self.analytics_affected_per_machine_load
        dest[
            "aurora_analytics_affected_per_machine_cpu_denorm"
        ] = self.analytics_affected_per_machine_cpu_denorm
        dest["aurora_txn_affected_cpu_denorm"] = self.txn_affected_cpu_denorm
        dest["aurora_pred_txn_peak_cpu_denorm"] = self.pred_txn_peak_cpu_denorm
        (
            dest["aurora_pred_txn_lat_s_p50"],
            dest["aurora_pred_txn_lat_s_p90"],
        ) = self.scaled_txn_lats
        dest.update(self.debug_values)


_AURORA_BASE_RESOURCE_VALUE = aurora_num_cpus(Provisioning("db.r6g.xlarge", 1))
