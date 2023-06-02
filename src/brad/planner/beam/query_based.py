import asyncio
import logging
from typing import List, Tuple, Dict

from brad.blueprint import Blueprint
from brad.blueprint.provisioning import MutableProvisioning
from brad.config.engine import Engine, EngineBitmapValues
from brad.planner import BlueprintPlanner
from brad.planner.workload.query import QueryRep
from brad.planner.scoring.provisioning import (
    compute_aurora_hourly_operational_cost,
    compute_redshift_hourly_operational_cost,
    compute_aurora_scan_cost,
    compute_athena_scan_cost,
    compute_aurora_transition_time_s,
    compute_redshift_transition_time_s,
)
from brad.planner.scoring.table_placement import (
    compute_athena_table_placement_cost,
    compute_table_movement_time_and_cost,
)
from brad.planner.workload import Workload
from brad.server.engine_connections import EngineConnections
from brad.utils.table_sizer import TableSizer

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

        # 3. Establish connections to the underlying engines (needed for scoring
        #    purposes). We use synchronous connections since there appears to be a
        #    bug in aioodbc that causes an indefinite await on a query result.
        #
        engine_connections = EngineConnections.connect_sync(
            self._config, self._schema_name, autocommit=False
        )

        try:
            # 4. Fetch any additional state needed for scoring.
            table_sizer = TableSizer(engine_connections, self._config)
            if next_workload.table_sizes_empty():
                next_workload.populate_table_sizes_using_blueprint(
                    self._current_blueprint, table_sizer
                )
                next_workload.set_dataset_size_from_table_sizes()
            ctx = _ScoringContext(self._current_blueprint, next_workload)

            # 5. Initialize the beam (top-k set) and other planning state.
            beam_size = (  # pylint: disable=unused-variable
                self._planner_config.beam_size()
            )
            engines = [Engine.Aurora, Engine.Redshift, Engine.Athena]
            first_query_idx = query_indices[0]
            current_top_k = []

            for routing_engine in engines:
                candidate = _BlueprintCandidate.based_on(self._current_blueprint)
                query_rep = analytical_queries[first_query_idx]
                # N.B. We must use the current blueprint because the tables
                # would not yet have been moved.
                query_rep.populate_data_accessed_mb(
                    for_engine=routing_engine,
                    connections=engine_connections,
                    blueprint=self._current_blueprint,
                )
                candidate.add_query(
                    first_query_idx, analytical_queries[first_query_idx], routing_engine
                )
                self._update_score(candidate, ctx)
                current_top_k.append(candidate)

            # 6. Run beam search.

            # 7. Return best blueprint.

        finally:
            engine_connections.close_sync()

    def _update_score(
        self, candidate: "_BlueprintCandidate", ctx: "_ScoringContext"
    ) -> None:
        # NOTE: This function recomputes the score components from scratch.
        # Based on how we construct and modify the blueprint candidates, we can
        # update the score incrementally as well. For implementation
        # convenience, we'll stick with recomputation until we know we need
        # incremental updates.

        analytical_queries = ctx.next_workload.analytical_queries()

        aurora_prov_cost = compute_aurora_hourly_operational_cost(
            candidate.aurora_provisioning
        )
        redshift_prov_cost = compute_redshift_hourly_operational_cost(
            candidate.redshift_provisioning
        )

        aurora_scan_cost = compute_aurora_scan_cost(
            map(
                lambda ql: analytical_queries[ql[0]],
                filter(lambda ql: ql[1] == Engine.Aurora, candidate.query_locations),
            ),
            self._planner_config,
        )
        athena_scan_cost = compute_athena_scan_cost(
            map(
                lambda ql: analytical_queries[ql[0]],
                filter(lambda ql: ql[1] == Engine.Athena, candidate.query_locations),
            ),
            self._planner_config,
        )

        athena_table_cost = compute_athena_table_placement_cost(
            candidate.table_placements, ctx.next_workload, self._planner_config
        )
        movement_score = compute_table_movement_time_and_cost(
            self._current_blueprint.table_locations_bitmap(),
            candidate.table_placements,
            ctx.next_workload,
            self._planner_config,
        )

        aurora_transition_time_s = compute_aurora_transition_time_s(
            ctx.current_blueprint.aurora_provisioning(),
            candidate.aurora_provisioning,
            self._planner_config,
        )
        redshift_transition_time_s = compute_redshift_transition_time_s(
            ctx.current_blueprint.redshift_provisioning(),
            candidate.redshift_provisioning,
            self._planner_config,
        )

        candidate.operational_cost = (
            aurora_prov_cost
            + redshift_prov_cost
            + athena_table_cost
            + aurora_scan_cost
            + athena_scan_cost
        )
        candidate.transition_cost = movement_score.movement_cost
        candidate.transition_time_s = (
            movement_score.movement_time_s
            + aurora_transition_time_s
            + redshift_transition_time_s
        )


class _ScoringContext:
    def __init__(self, current_blueprint: Blueprint, next_workload: Workload) -> None:
        self.current_blueprint = current_blueprint
        self.next_workload = next_workload


class _BlueprintCandidate:
    """
    A "barebones" representation of a blueprint, used during the optimization
    process.
    """

    @classmethod
    def based_on(cls, blueprint: Blueprint) -> "_BlueprintCandidate":
        return cls(
            blueprint.aurora_provisioning().mutable_clone(),
            blueprint.redshift_provisioning().mutable_clone(),
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
