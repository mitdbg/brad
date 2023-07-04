from typing import Dict, List

from brad.config.engine import Engine
from brad.blueprint import Blueprint
from brad.config.planner import PlannerConfig
from brad.planner.metrics import Metrics
from brad.planner.workload import Workload
from brad.routing.router import Router


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

        self.current_latency_weights: Dict[Engine, float] = {}

    def simulate_current_workload_routing(self, router: Router) -> None:
        self.current_query_locations[Engine.Aurora].clear()
        self.current_query_locations[Engine.Redshift].clear()
        self.current_query_locations[Engine.Athena].clear()

        all_queries = self.current_workload.analytical_queries()
        for qidx, query in enumerate(all_queries):
            eng = router.engine_for_sync(query)
            self.current_query_locations[eng].append(qidx)

    def compute_engine_latency_weights(self) -> None:
        for engine in [Engine.Aurora, Engine.Redshift, Engine.Athena]:
            if len(self.current_query_locations[engine]) == 0:
                # Avoid having an explicit entry for engines that receive no
                # queries (the engine could be off).
                continue

            self.current_latency_weights[
                engine
            ] = self.current_workload.get_predicted_analytical_latency_batch(
                self.current_query_locations[engine], engine
            ).sum()
