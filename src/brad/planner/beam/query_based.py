import asyncio
import logging
from typing import List, Tuple, Dict

from brad.blueprint import Blueprint
from brad.blueprint.provisioning import MutableProvisioning
from brad.blueprint.table import Table
from brad.config.engine import Engine, EngineBitmapValues
from brad.planner import BlueprintPlanner
from brad.planner.workload.query import QueryRep

logger = logging.getLogger(__name__)


class QueryBasedBeamPlanner(BlueprintPlanner):
    async def run_forever(self) -> None:
        while True:
            await asyncio.sleep(3)
            logger.debug("Planner is checking if a replan is needed...")
            if self._check_if_metrics_warrant_replanning():
                await self.run_replan()

    def _check_if_metrics_warrant_replanning(self) -> bool:
        # See if the metrics indicate that we should trigger the planning
        # process.
        return False

    async def run_replan(self) -> None:
        logger.info("Running a replan...")

        # 1. Fetch the next workload and make query execution predictions.
        next_workload = self._workload_provider.next_workload()
        self._analytics_latency_scorer.apply_predicted_latencies(next_workload)

        # 2. Compute query gains and reorder queries by their gain in descending
        # order.
        gains = next_workload.compute_latency_gains()
        analytical_queries = next_workload.analytical_queries()
        query_indices = list(range(len(next_workload.analytical_queries())))
        query_indices.sort(key=lambda idx: gains[idx], reverse=True)

        # Sanity check. We cannot run planning without at least one query in the
        # workload.
        assert len(query_indices) > 0

        # 3. Initialize the beam (top-k set).
        beam_size = self._planner_config.beam_size()
        engines = [Engine.Aurora, Engine.Redshift, Engine.Athena]
        first_query_idx = query_indices[0]
        current_top_k = []

        for routing_engine in engines:
            candidate = _BlueprintCandidate.based_on(self._current_blueprint)
            candidate.add_query(
                first_query_idx, analytical_queries[first_query_idx], routing_engine
            )
            current_top_k.append(candidate)

        # Score the blueprint components.

        # 4. Run beam search.
        for next_query_idx in query_indices[1:]:
            next_top_k = []

            for candidate in current_top_k:
                pass

        # 5. Return best blueprint.



class _ScoringContext:
    @classmethod
    def from_source(cls, blueprint: Blueprint) -> "_ScoringContext":
        return cls()

    def __init__(self) -> None:
        pass


class _BlueprintCandidate:
    """
    A "barebones" representation of a blueprint, used during the optimization
    process.
    """

    @classmethod
    def based_on(cls, blueprint: Blueprint) -> "_BlueprintCandidate":
        return cls(
            blueprint.aurora_provisioning(),
            blueprint.redshift_provisioning(),
            {t.name: 0 for t in blueprint.tables()},
        )

    def __init__(
        self,
        aurora: MutableProvisioning,
        redshift: MutableProvisioning,
        table_placements: Dict[str, int],
    ) -> None:
        self.query_locations: List[Tuple[int, Engine]] = []
        # Table locations are represented using a bitmap. We initialize each
        # table to being present on no engines.
        self.table_placements = table_placements
        self.aurora_provisioning = aurora.mutable_clone()
        self.redshift_provisioning = redshift.mutable_clone()

        # Scoring components.
        self.operational_cost = 0.0
        self.transition_cost = 0.0
        self.transition_time_s = 0.0

    def add_query(self, query_idx: int, query: QueryRep, location: Engine) -> None:
        self.query_locations.append((query_idx, location))
        engine_bitvalue = EngineBitmapValues[location]

        # Ensure that the table is present on the engine on which we want to run
        # the query.
        for table_name in query.tables():
            try:
                self.table_placements[table_name] |= engine_bitvalue
            except KeyError:
                # Some of the tables returned are not tables but names of CTEs.
                pass

    def clone(self) -> "_BlueprintCandidate":
        return _BlueprintCandidate(
            self.aurora_provisioning.mutable_clone(),
            self.redshift_provisioning.mutable_clone(),
            self.table_placements.copy(),
        )
