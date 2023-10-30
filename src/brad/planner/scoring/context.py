import logging
import numpy as np
from typing import Dict, List

from brad.config.engine import Engine
from brad.blueprint import Blueprint
from brad.config.planner import PlannerConfig
from brad.planner.metrics import Metrics
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

        self.already_logged_txn_interference_warning = False

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
        for engine in [Engine.Aurora, Engine.Redshift, Engine.Athena]:
            if len(self.current_query_locations[engine]) == 0:
                # Avoid having an explicit entry for engines that receive no
                # queries (the engine could be off).
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

            # N.B. Using the actual recorded run times is slightly problematic
            # here. We use this normalization factor as a part of predicting a
            # query's execution time on a different blueprint. We need to use
            # query execution times on the same provisioning (and system load)
            # as the recorded run times. Scaling the recorded run times to a
            # base provisioning or vice-versa is difficult to do accurately
            # without having this normalization factor.

            # 2. Extract query weights (based on arrival frequency) and scale
            # the run times.
            query_weights = self.current_workload.get_arrival_counts_batch(
                self.current_query_locations[engine]
            )
            assert query_weights.shape == predicted_base_latencies.shape

            self.engine_latency_norm_factor[engine] = np.dot(
                predicted_base_latencies, query_weights
            )
