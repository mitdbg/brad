import logging
import numpy as np
from typing import Dict, List

from brad.config.engine import Engine
from brad.blueprint import Blueprint
from brad.config.planner import PlannerConfig
from brad.planner.metrics import Metrics
from brad.planner.scoring.performance.unified_aurora import AuroraProvisioningScore
from brad.planner.scoring.performance.unified_redshift import RedshiftProvisioningScore
from brad.planner.scoring.provisioning import redshift_num_cpus
from brad.planner.workload import Workload
from brad.routing.router import Router

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

        # This is used for reweighing metrics due to query routing changes
        # across blueprints.
        self.engine_latency_norm_factor: Dict[Engine, float] = {}

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
                maybe_eng = query.primary_execution_location()
                if maybe_eng is not None:
                    self.current_query_locations[maybe_eng].append(qidx)

            # Fall back to the router if the historical routing location is not
            # available.
            eng = await router.engine_for(query)
            self.current_query_locations[eng].append(qidx)

    def compute_engine_latency_norm_factor(self) -> None:
        use_recorded_run_time_if_available = self.planner_config.flag(
            "use_recorded_run_time_if_available", default=False
        )

        for engine in [Engine.Aurora, Engine.Redshift, Engine.Athena]:
            if len(self.current_query_locations[engine]) == 0:
                # Avoid having an explicit entry for engines that receive no
                # queries (the engine could be off).
                continue

            queries = self.current_workload.analytical_queries()

            # 1. Get predicted base latencies.
            predicted_base_latencies = (
                self.current_workload.get_predicted_analytical_latency_batch(
                    self.current_query_locations[engine], engine
                )
            )

            # 2. Scale the predictions to the current provisioning, if applicable
            if engine == Engine.Aurora:
                aurora_prov = self.current_blueprint.aurora_provisioning()
                if aurora_prov.num_nodes() > 1:
                    # There are read replicas
                    aurora_load = self.metrics.aurora_reader_load_minute_avg
                else:
                    # No read replicas.
                    aurora_load = self.metrics.aurora_writer_load_minute_avg
                pred_prov_latencies = (
                    AuroraProvisioningScore.query_latency_load_resources(
                        predicted_base_latencies, aurora_prov, aurora_load, self
                    )
                )

            elif engine == Engine.Redshift:
                redshift_prov = self.current_blueprint.redshift_provisioning()
                redshift_cpu_denorm = (
                    redshift_prov.num_nodes()
                    * redshift_num_cpus(redshift_prov)
                    * self.metrics.redshift_cpu_avg
                    / 100.0
                )
                pred_prov_latencies = RedshiftProvisioningScore.scale_load_resources(
                    predicted_base_latencies,
                    self.current_blueprint.redshift_provisioning(),
                    redshift_cpu_denorm,
                    self,
                )

            else:
                # Athena: No changes needed.
                pred_prov_latencies = predicted_base_latencies

            # 3. Use the recorded run times, if applicable.
            if use_recorded_run_time_if_available:
                # Extract the recorded run time.
                recorded_times = np.zeros_like(pred_prov_latencies)
                recorded_times += np.nan

                for qidx, query in enumerate(queries):
                    past_execs = query.past_executions()
                    if past_execs is None or len(past_execs) == 0:
                        continue
                    this_engine_rt_s = np.array(
                        [
                            exec_rt_s
                            for exec_engine, exec_rt_s in past_execs
                            if exec_engine == engine
                        ]
                    )
                    if len(this_engine_rt_s) == 0:
                        logger.warning(
                            "No recorded run times for query index %d on %s",
                            qidx,
                            engine,
                        )
                        continue
                    recorded_times[qidx] = this_engine_rt_s.mean()

                # Use the predictions if a recorded run time is not available.
                pred_prov_latencies = np.where(
                    np.isnan(recorded_times), pred_prov_latencies, recorded_times
                )

            # 3. Extract query weights (based on arrival frequency) and scale
            # the run times.
            query_weights = np.array([q.arrival_count() for q in queries])
            assert query_weights.shape == pred_prov_latencies.shape

            self.engine_latency_norm_factor[engine] = np.dot(
                pred_prov_latencies, query_weights
            )
