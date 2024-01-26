import logging
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import timedelta

from brad.config.engine import Engine
from brad.blueprint import Blueprint
from brad.config.planner import PlannerConfig
from brad.planner.enumeration.provisioning import ProvisioningEnumerator
from brad.planner.metrics import Metrics
from brad.planner.workload import Workload
from brad.routing.router import Router
from brad.planner.scoring.provisioning import compute_athena_scan_cost_numpy
from brad.planner.scoring.provisioning import (
    compute_aurora_hourly_operational_cost,
    compute_redshift_hourly_operational_cost,
)
from brad.planner.scoring.performance.unified_aurora import AuroraProvisioningScore
from brad.planner.scoring.performance.unified_redshift import RedshiftProvisioningScore
from brad.planner.scoring.table_placement import (
    compute_single_athena_table_cost,
    compute_single_aurora_table_cost,
    compute_single_table_movement_time_and_cost,
    TableMovementScore,
)

logger = logging.getLogger(__name__)


class ScoringContext:
    """
    A wrapper class used to collect the components needed for blueprint scoring.
    """

    def __init__(
        self,
        schema_name: str,
        current_blueprint: Blueprint,
        current_workload: Workload,
        next_workload: Workload,
        metrics: Metrics,
        planner_config: PlannerConfig,
    ) -> None:
        self.schema_name = schema_name
        self.current_blueprint = current_blueprint
        self.current_workload = current_workload
        self.next_workload = next_workload
        self.metrics = metrics
        self.planner_config = planner_config

        self.current_query_locations: Dict[Engine, List[int]] = {}
        self.current_query_locations[Engine.Aurora] = []
        self.current_query_locations[Engine.Redshift] = []
        self.current_query_locations[Engine.Athena] = []

        # TODO: This is messy - we should have one place for blueprint scoring
        # relative to a workload.
        self.current_workload_predicted_hourly_scan_cost = 0.0
        self.current_blueprint_provisioning_hourly_cost = 0.0

        # This is used for reweighing metrics due to query routing changes
        # across blueprints.
        self.engine_latency_norm_factor: Dict[Engine, float] = {}

        self.already_logged_txn_interference_warning = False

        # Used to memoize this value instead of recomputing it as it is a
        # function of the CPU utilization values.
        self.cpu_skew_adjustment: Optional[float] = None

        # These are computed once and re-used to avoid repeated recomputation
        # during the optimization.
        self.table_storage_costs: Dict[Tuple[str, Engine], float] = {}
        self.table_movement: Dict[Tuple[str, Engine], TableMovementScore] = {}

    async def simulate_current_workload_routing(self, router: Router) -> None:
        self.current_query_locations[Engine.Aurora].clear()
        self.current_query_locations[Engine.Redshift].clear()
        self.current_query_locations[Engine.Athena].clear()

        use_recorded_routing_if_available = self.planner_config.flag(
            "use_recorded_routing_if_available", default=False
        )

        all_queries = self.current_workload.analytical_queries()
        for qidx, query in enumerate(all_queries):
            if use_recorded_routing_if_available:
                maybe_eng = query.most_recent_execution_location()
                if maybe_eng is not None:
                    self.current_query_locations[maybe_eng].append(qidx)
                    continue

            # Fall back to the router if the historical routing location is not
            # available.
            eng = await router.engine_for(query)
            self.current_query_locations[eng].append(qidx)

    def compute_current_workload_predicted_hourly_scan_cost(self) -> None:
        # Note that this is not ideal. We should use the actual _recorded_ scan
        # cost (but this is not readily recorded through PyAthena).
        # Athena bytes:
        bytes_accessed = (
            self.current_workload.get_predicted_athena_bytes_accessed_batch(
                self.current_query_locations[Engine.Athena]
            )
        )
        arrival_counts = self.current_workload.get_arrival_counts_batch(
            self.current_query_locations[Engine.Athena]
        )
        period_scan_cost = compute_athena_scan_cost_numpy(
            bytes_accessed, arrival_counts, self.planner_config
        )
        scaling_factor = (
            timedelta(hours=1).total_seconds()
            / self.current_workload.period().total_seconds()
        )
        self.current_workload_predicted_hourly_scan_cost = (
            period_scan_cost * scaling_factor
        )
        if not self.planner_config.use_io_optimized_aurora():
            logger.warning("Aurora blocks accessed is not implemented.")

    def compute_current_blueprint_provisioning_hourly_cost(self) -> None:
        aurora_cost = compute_aurora_hourly_operational_cost(
            self.current_blueprint.aurora_provisioning(), self
        )
        redshift_cost = compute_redshift_hourly_operational_cost(
            self.current_blueprint.redshift_provisioning()
        )
        self.current_blueprint_provisioning_hourly_cost = aurora_cost + redshift_cost

    def compute_workload_provisioning_predictions(self) -> None:
        aurora_enumerator = ProvisioningEnumerator(Engine.Aurora)
        aurora_it = aurora_enumerator.enumerate_nearby(
            self.current_blueprint.aurora_provisioning(),
            self.planner_config.aurora_provisioning_search_distance(),
            # aurora_enumerator.scaling_to_distance(
            #     self.current_blueprint.aurora_provisioning(),
            #     self.planner_config.max_provisioning_multiplier(),
            #     Engine.Aurora,
            # ),
        )
        self.next_workload.precomputed_aurora_analytical_latencies = (
            AuroraProvisioningScore.predict_query_latency_resources_batch(
                self.next_workload.get_predicted_analytical_latency_all(Engine.Aurora),
                aurora_it,
                self,
            )
        )

        redshift_enumerator = ProvisioningEnumerator(Engine.Redshift)
        redshift_it = redshift_enumerator.enumerate_nearby(
            self.current_blueprint.redshift_provisioning(),
            self.planner_config.redshift_provisioning_search_distance(),
            # redshift_enumerator.scaling_to_distance(
            #     self.current_blueprint.redshift_provisioning(),
            #     self.planner_config.max_provisioning_multiplier(),
            #     Engine.Redshift,
            # ),
        )
        self.next_workload.precomputed_redshift_analytical_latencies = (
            RedshiftProvisioningScore.predict_query_latency_resources_batch(
                self.next_workload.get_predicted_analytical_latency_all(
                    Engine.Redshift
                ),
                redshift_it,
                self,
            )
        )

    def compute_engine_latency_norm_factor(self) -> None:
        for engine in [Engine.Aurora, Engine.Redshift, Engine.Athena]:
            if len(self.current_query_locations[engine]) == 0:
                # Avoid having an explicit entry for engines that receive no
                # queries (the engine could be off).
                continue

            if (
                engine == Engine.Redshift
                and self.current_blueprint.redshift_provisioning().num_nodes() == 0
            ):
                # Avoid having an entry for engines that are off. The check
                # above might pass if we are using a long planning window and
                # queries executed on Redshift in the past.
                continue

            if (
                engine == Engine.Aurora
                and self.current_blueprint.aurora_provisioning().num_nodes() == 0
            ):
                # Avoid having an entry for engines that are off. The check
                # above might pass if we are using a long planning window and
                # queries executed on Aurora in the past.
                continue

            all_queries = self.current_workload.analytical_queries()
            relevant_queries = []
            for qidx in self.current_query_locations[engine]:
                relevant_queries.append(all_queries[qidx])

            # 1. Get predicted base latencies.
            predicted_base_latencies = (
                self.current_workload.get_predicted_analytical_latency_batch(
                    self.current_query_locations[engine], engine
                )
            )

            # 2. Adjust to the provisioning if needed.
            if engine == Engine.Aurora:
                adjusted_latencies = (
                    AuroraProvisioningScore.predict_query_latency_resources(
                        predicted_base_latencies,
                        self.current_blueprint.aurora_provisioning(),
                        self,
                    )
                )
            elif engine == Engine.Redshift:
                adjusted_latencies = (
                    RedshiftProvisioningScore.predict_query_latency_resources(
                        predicted_base_latencies,
                        self.current_blueprint.redshift_provisioning(),
                        self,
                    )
                )
            elif engine == Engine.Athena:
                # No provisioning.
                adjusted_latencies = predicted_base_latencies

            # 3. Extract query weights (based on arrival frequency) and scale
            # the run times.
            query_weights = self.current_workload.get_arrival_counts_batch(
                self.current_query_locations[engine]
            )
            assert query_weights.shape == predicted_base_latencies.shape

            self.engine_latency_norm_factor[engine] = np.dot(
                adjusted_latencies, query_weights
            )

    def compute_table_transitions(self) -> None:
        self.table_storage_costs.clear()
        self.table_movement.clear()
        for table in self.current_blueprint.tables():
            self.table_storage_costs[
                (table.name, Engine.Athena)
            ] = compute_single_athena_table_cost(table.name, self)
            # You only pay for 1 copy of the table on Aurora, regardless of
            # how many read replicas you have.
            self.table_storage_costs[
                (table.name, Engine.Aurora)
            ] = compute_single_aurora_table_cost(table.name, self)

            curr = self.current_blueprint.table_locations_bitmap()[table.name]

            for engine, bit_mask in Workload.EngineLatencyIndex.items():
                result = compute_single_table_movement_time_and_cost(
                    table.name, curr, curr | bit_mask, self
                )
                self.table_movement[(table.name, engine)] = result

    def correct_predictions_based_on_observations(self) -> None:
        redshift_preds_orig = self.next_workload.get_predicted_analytical_latency_all(
            Engine.Redshift
        )
        obs, obs_locs, obs_idx = self.next_workload.get_query_observations()
        if obs_idx.shape[0] == 0:
            logger.info("No queries in the workload.")
            return

        # Process Redshift.
        is_redshift = np.where(obs_locs == Workload.EngineLatencyIndex[Engine.Redshift])
        if is_redshift[0].sum() > 0:
            redshift_obs = obs[is_redshift]
            redshift_qidx = obs_idx[is_redshift]
            redshift_preds = redshift_preds_orig[redshift_qidx]
            base = RedshiftProvisioningScore.predict_base_latency(
                redshift_obs, self.current_blueprint.redshift_provisioning(), self
            )
            ratio = redshift_preds / base
            # Queries where we have observations where the predictions are probably
            # 5x larger and the predictions violate the SLOs.
            hes = np.where((ratio > 3.0) & (redshift_preds > 30.0))
            redshift_to_replace = redshift_qidx[hes]
            logger.info(
                "[Redshift Prediction Corrections] Replacing %d base predictions.",
                len(redshift_to_replace),
            )
            if len(redshift_to_replace) > 0:
                self.next_workload.apply_predicted_latency_corrections(
                    Engine.Redshift, redshift_to_replace, base[hes]
                )
                for i, qidx in enumerate(redshift_to_replace):
                    logger.info(
                        "Replacing Redshift %d -- %s -- %.4f (Orig. pred. %.4f)",
                        qidx,
                        str(self.next_workload.lookup_query_for_debugging(qidx)),
                        base[hes][i],
                        redshift_preds[hes][i],
                    )

        # Process Aurora.
        aurora_preds_orig = self.next_workload.get_predicted_analytical_latency_all(
            Engine.Aurora
        )
        is_aurora = np.where(obs_locs == Workload.EngineLatencyIndex[Engine.Aurora])
        if is_aurora[0].sum() > 0:
            aurora_obs = obs[is_aurora]
            aurora_qidx = obs_idx[is_aurora]
            aurora_preds = aurora_preds_orig[aurora_qidx]
            aurora_base = AuroraProvisioningScore.predict_base_latency(
                aurora_obs, self.current_blueprint.aurora_provisioning(), self
            )
            aurora_ratio = aurora_preds / aurora_base
            ahes = np.where((aurora_ratio > 5.0) & (aurora_preds > 30.0))
            aurora_to_replace = aurora_qidx[ahes]
            logger.info(
                "[Aurora Prediction Corrections] Replacing %d base predictions.",
                len(aurora_to_replace),
            )
            if len(aurora_to_replace) > 0:
                self.next_workload.apply_predicted_latency_corrections(
                    Engine.Aurora, aurora_to_replace, aurora_base[ahes]
                )
                for i, qidx in enumerate(aurora_to_replace):
                    logger.info(
                        "Replacing Aurora %d -- %s -- %.4f (Orig. pred. %.4f)",
                        qidx,
                        str(self.next_workload.lookup_query_for_debugging(qidx)),
                        aurora_base[ahes][i],
                        aurora_preds[ahes][i],
                    )
