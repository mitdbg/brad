import asyncio
import enum
import heapq
import json
import logging
import numpy as np
import numpy.typing as npt
from typing import Any, List, Dict, Optional

from brad.blueprint import Blueprint
from brad.blueprint.provisioning import Provisioning, MutableProvisioning
from brad.config.engine import Engine, EngineBitmapValues
from brad.planner import BlueprintPlanner
from brad.planner.compare.blueprint import ComparableBlueprint
from brad.planner.compare.function import BlueprintComparator
from brad.planner.debug_logger import BlueprintPlanningDebugLogger
from brad.planner.enumeration.provisioning import ProvisioningEnumerator
from brad.planner.workload.query import Query
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.provisioning import (
    compute_aurora_hourly_operational_cost,
    compute_redshift_hourly_operational_cost,
    compute_aurora_scan_cost,
    compute_athena_scan_cost,
    compute_aurora_transition_time_s,
    compute_redshift_transition_time_s,
    aurora_resource_value,
    redshift_resource_value,
)
from brad.planner.scoring.table_placement import (
    compute_single_athena_table_cost,
    compute_single_table_movement_time_and_cost,
)
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
            ctx = ScoringContext(
                self._current_blueprint,
                self._current_workload,
                next_workload,
                self._planner_config,
            )

            # 5. Initialize planning state.
            beam_size = self._planner_config.beam_size()
            engines = [Engine.Aurora, Engine.Redshift, Engine.Athena]
            first_query_idx = query_indices[0]
            current_top_k: List[_BlueprintCandidate] = []

            # Not a fundamental limitation, but it simplifies the implementation
            # below if this condition is true.
            assert beam_size >= len(engines)

            # 6. Initialize the top-k set (beam).
            for routing_engine in engines:
                candidate = _BlueprintCandidate.based_on(
                    self._current_blueprint, self._comparator
                )
                candidate.add_transactional_tables(ctx)
                query = analytical_queries[first_query_idx]
                # N.B. We must use the current blueprint because the tables
                # would not yet have been moved.
                query.populate_data_accessed_mb(
                    for_engine=routing_engine,
                    connections=engine_connections,
                    blueprint=self._current_blueprint,
                )
                candidate.add_query(
                    first_query_idx,
                    query,
                    routing_engine,
                    next_workload.get_predicted_analytical_latency(
                        first_query_idx, routing_engine
                    ),
                    ctx,
                )
                candidate.check_feasibility()

                if candidate.feasibility == _BlueprintFeasibility.Infeasible:
                    candidate.find_best_provisioning(ctx)
                if candidate.feasibility == _BlueprintFeasibility.Infeasible:
                    continue

                candidate.recompute_provisioning_dependent_scoring(ctx)
                current_top_k.append(candidate)

            if len(current_top_k) == 0:
                logger.error(
                    "Query-based beam blueprint planning failed. Could not generate an initial set of feasible blueprints."
                )
                return

            # 7. Run beam search to formulate the table placements.
            for j, query_idx in enumerate(query_indices[1:]):
                if j % 100 == 0:
                    logger.debug("Processing index %d of %d", j, len(query_indices[1:]))

                next_top_k: List[_BlueprintCandidate] = []
                query = analytical_queries[first_query_idx]

                # For each candidate in the current top k, expand it by one
                # query in the workload.
                for curr_candidate in current_top_k:
                    for routing_engine in engines:
                        # N.B. We must use the current blueprint because the tables
                        # would not yet have been moved.
                        query.populate_data_accessed_mb(
                            for_engine=routing_engine,
                            connections=engine_connections,
                            blueprint=self._current_blueprint,
                        )
                        next_candidate = curr_candidate.clone()
                        next_candidate.add_query(
                            query_idx,
                            query,
                            routing_engine,
                            next_workload.get_predicted_analytical_latency(
                                query_idx, routing_engine
                            ),
                            ctx,
                        )

                        # Make sure this candidate is feasible (otherwise skip it).
                        next_candidate.check_feasibility()
                        if (
                            next_candidate.feasibility
                            == _BlueprintFeasibility.Infeasible
                        ):
                            next_candidate.find_best_provisioning(ctx)
                        if (
                            next_candidate.feasibility
                            == _BlueprintFeasibility.Infeasible
                        ):
                            continue

                        next_candidate.recompute_provisioning_dependent_scoring(ctx)

                        if len(next_top_k) < beam_size:
                            next_top_k.append(next_candidate)
                            if len(next_top_k) == beam_size:
                                # Meant to be a max heap. A lower score is
                                # better, so we need to keep around the highest
                                # scoring candidate.
                                heapq.heapify(next_top_k)
                        else:
                            # Need to eliminate a blueprint candidate.
                            if not (next_candidate.is_better_than(next_top_k[0])):
                                # The candidate is worse than the current worst top-k blueprint.
                                # Check if a better provisioning improves the candidate's score.
                                next_candidate.find_best_provisioning(ctx)

                            if not (next_candidate.is_better_than(next_top_k[0])):
                                # We eliminate `next_candidate`. Even after
                                # looking for the best provisioning, it has a
                                # worse score compared to `next_top_k[0]` (the
                                # worst scoring blueprint candidate in the
                                # top-k).
                                continue

                            while (
                                next_candidate.is_better_than(next_top_k[0])
                                and not next_top_k[0].explored_provisionings
                            ):
                                # Being in this loop means that the next
                                # candidate is better than the worst candidate
                                # in the current top k, but we have not tuned
                                # the worst top k blueprint's provisioning.
                                current_worst = heapq.heappop(next_top_k)
                                current_worst.find_best_provisioning(ctx)
                                heapq.heappush(next_top_k, current_worst)

                            if next_candidate.is_better_than(next_top_k[0]):
                                heapq.heappushpop(next_top_k, next_candidate)

                current_top_k = next_top_k

            # Log the placement top k for debugging purposes, if needed.
            placement_top_k_logger = BlueprintPlanningDebugLogger.create_if_requested(
                "query_beam_placement_topk"
            )
            if placement_top_k_logger is not None:
                for candidate in current_top_k:
                    placement_top_k_logger.log_debug_values(candidate.to_debug_values())

            # 8. Run a final greedy search over provisionings in the top-k set.
            final_top_k: List[_BlueprintCandidate] = []

            for candidate in current_top_k:
                aurora_enumerator = ProvisioningEnumerator(Engine.Aurora)
                aurora_it = aurora_enumerator.enumerate_nearby(
                    ctx.current_blueprint.aurora_provisioning(),
                    aurora_enumerator.scaling_to_distance(
                        ctx.current_blueprint.aurora_provisioning(),
                        ctx.planner_config.max_provisioning_multiplier(),
                    ),
                )

                redshift_enumerator = ProvisioningEnumerator(Engine.Redshift)
                redshift_it = redshift_enumerator.enumerate_nearby(
                    ctx.current_blueprint.redshift_provisioning(),
                    redshift_enumerator.scaling_to_distance(
                        ctx.current_blueprint.redshift_provisioning(),
                        ctx.planner_config.max_provisioning_multiplier(),
                    ),
                )

                for aurora in aurora_it:
                    for redshift in redshift_it:
                        new_candidate = candidate.clone()
                        new_candidate.update_aurora_provisioning(aurora)
                        new_candidate.update_redshift_provisioning(redshift)
                        new_candidate.check_feasibility()
                        if (
                            new_candidate.feasibility
                            == _BlueprintFeasibility.Infeasible
                        ):
                            continue
                        new_candidate.recompute_provisioning_dependent_scoring(ctx)

                        if len(final_top_k) < beam_size:
                            final_top_k.append(new_candidate)
                            if len(final_top_k) == beam_size:
                                heapq.heapify(final_top_k)
                        elif new_candidate.is_better_than(final_top_k[0]):
                            heapq.heappushpop(final_top_k, new_candidate)

            # Best blueprint will be ordered first (we have a negated `__lt__`
            # method to work with `heapq` to create a max heap).
            final_top_k.sort(reverse=True)

            if len(final_top_k) == 0:
                logger.error(
                    "The query-based beam planner failed to find any feasible blueprints."
                )
                return

            # Log the final top k for debugging purposes, if needed.
            final_top_k_logger = BlueprintPlanningDebugLogger.create_if_requested(
                "query_beam_final_topk"
            )
            if final_top_k_logger is not None:
                for candidate in final_top_k:
                    final_top_k_logger.log_debug_values(candidate.to_debug_values())

            # TODO: Consider re-ranking the final top k using the actual query
            # routing policy.
            best_candidate = final_top_k[0]

            # 9. Touch up the table placements. Add any missing tables to ensure
            #    we do not have data loss.
            for tbl, placement_bitmap in best_candidate.table_placements.items():
                if placement_bitmap != 0:
                    continue
                # Put the table on Athena (this is a heuristic: we assume the
                # table is rarely accessed).
                best_candidate.table_placements[tbl] |= EngineBitmapValues[
                    Engine.Athena
                ]

            # 10. Output the new blueprint.
            best_blueprint = best_candidate.to_blueprint()
            self._current_blueprint = best_blueprint
            self._current_workload = next_workload
            await self._notify_new_blueprint(best_blueprint)

            logger.info("Selected blueprint:")
            logger.info("%s", best_blueprint)

            debug_values = best_candidate.to_debug_values()
            logger.debug("Selected blueprint details: %s", json.dumps(debug_values, indent=2))

        finally:
            engine_connections.close_sync()


class _BlueprintFeasibility(enum.Enum):
    Unchecked = 0
    Feasible = 1
    Infeasible = 2


class _BlueprintCandidate(ComparableBlueprint):
    """
    A "barebones" representation of a blueprint, used during the optimization
    process.
    """

    @classmethod
    def based_on(
        cls, blueprint: Blueprint, comparator: BlueprintComparator
    ) -> "_BlueprintCandidate":
        return cls(
            blueprint,
            blueprint.aurora_provisioning().mutable_clone(),
            blueprint.redshift_provisioning().mutable_clone(),
            {t.name: 0 for t in blueprint.tables()},
            comparator,
        )

    def __init__(
        self,
        source: Blueprint,
        aurora: MutableProvisioning,
        redshift: MutableProvisioning,
        table_placements: Dict[str, int],
        comparator: BlueprintComparator,
    ) -> None:
        self.aurora_provisioning = aurora.mutable_clone()
        self.redshift_provisioning = redshift.mutable_clone()
        # Table locations are represented using a bitmap. We initialize each
        # table to being present on no engines.
        self.table_placements = table_placements

        self._source_blueprint = source
        self._comparator = comparator

        self.query_locations: Dict[Engine, List[int]] = {}
        self.query_locations[Engine.Aurora] = []
        self.query_locations[Engine.Redshift] = []
        self.query_locations[Engine.Athena] = []

        self.base_query_latencies: Dict[Engine, List[float]] = {}
        self.base_query_latencies[Engine.Aurora] = []
        self.base_query_latencies[Engine.Redshift] = []
        self.base_query_latencies[Engine.Athena] = []

        # Scoring components.

        # Monetary costs.
        self.provisioning_cost = 0.0
        self.storage_cost = 0.0
        self.workload_scan_cost = 0.0
        self.table_movement_trans_cost = 0.0

        # Transition times.
        self.table_movement_trans_time_s = 0.0
        self.provisioning_trans_time_s = 0.0

        # Used for scoring purposes.
        self.explored_provisionings = False
        self.feasibility = _BlueprintFeasibility.Unchecked
        self.scaled_query_latencies: Dict[Engine, npt.NDArray] = {}

        # Used during comparisons.
        self._memoized: Dict[str, Any] = {}

    def to_blueprint(self) -> Blueprint:
        # We use the source blueprint for table schema information.
        return Blueprint(
            self._source_blueprint.schema_name(),
            self._source_blueprint.tables(),
            self.get_table_placement(),
            self.aurora_provisioning.clone(),
            self.redshift_provisioning.clone(),
            self._source_blueprint.router_provider(),
        )

    def to_debug_values(self) -> Dict[str, int | float | str]:
        values: Dict[str, int | float | str] = {}

        # Provisioning.
        values["aurora_instance"] = self.aurora_provisioning.instance_type()
        values["aurora_nodes"] = self.aurora_provisioning.num_nodes()
        values["redshift_instance"] = self.redshift_provisioning.instance_type()
        values["redshift_nodes"] = self.redshift_provisioning.num_nodes()

        # Tables placements.
        for tbl, bitmap in self.table_placements.items():
            values["table_{}".format(tbl)] = bitmap

        # Query breakdowns (rough).
        values["aurora_queries"] = len(self.query_locations[Engine.Aurora])
        values["redshift_queries"] = len(self.query_locations[Engine.Redshift])
        values["athena_queries"] = len(self.query_locations[Engine.Athena])

        # Scoring components.
        values["provisioning_cost"] = self.provisioning_cost
        values["storage_cost"] = self.storage_cost
        values["workload_scan_cost"] = self.workload_scan_cost
        values["table_movement_trans_cost"] = self.table_movement_trans_cost
        values["table_movement_trans_time_s"] = self.table_movement_trans_time_s
        values["provisioning_trans_time_s"] = self.provisioning_trans_time_s

        return values

    def add_query(
        self,
        query_idx: int,
        query: Query,
        location: Engine,
        base_latency: float,
        ctx: ScoringContext,
    ) -> None:
        self.query_locations[location].append(query_idx)
        self.base_query_latencies[location].append(base_latency)
        engine_bitvalue = EngineBitmapValues[location]

        # Ensure that the table is present on the engine on which we want to run
        # the query.
        table_diffs = []
        for table_name in query.tables():
            try:
                orig = self.table_placements[table_name]
                self.table_placements[table_name] |= engine_bitvalue

                if orig != self.table_placements[table_name]:
                    table_diffs.append((table_name, self.table_placements[table_name]))

            except KeyError:
                # Some of the tables returned are not tables but names of CTEs.
                pass

        # Scan monetary costs that this query imposes.
        if location == Engine.Athena:
            self.workload_scan_cost += compute_athena_scan_cost(
                [query], ctx.planner_config
            )
        elif location == Engine.Aurora:
            self.workload_scan_cost += compute_aurora_scan_cost(
                [query], ctx.planner_config
            )

        # Table movement costs that this query imposes.
        for name, next_placement in table_diffs:
            curr = ctx.current_blueprint.table_locations_bitmap()[name]
            if ((~curr) & next_placement) == 0:
                # This table was already present on the engine.
                continue

            result = compute_single_table_movement_time_and_cost(
                name,
                curr,
                next_placement,
                ctx.current_workload,
                ctx.planner_config,
            )
            self.table_movement_trans_cost += result.movement_cost
            self.table_movement_trans_time_s += result.movement_time_s

            # If we added a table to Athena, we need to take into account its
            # storage costs.
            if (((~curr) & next_placement) & (EngineBitmapValues[Engine.Athena])) != 0:
                # We added the table to Athena.
                self.storage_cost += compute_single_athena_table_cost(
                    name, ctx.next_workload, ctx.planner_config
                )

        # Adding a new query can affect the feasibility of the provisioning.
        self.feasibility = _BlueprintFeasibility.Unchecked
        self.explored_provisionings = False
        self._memoized.clear()

    def add_transactional_tables(self, ctx: ScoringContext) -> None:
        referenced_tables = set()

        # Make sure that tables referenced in transactions are present on
        # Aurora.
        for query in ctx.next_workload.transactional_queries():
            for tbl in query.tables():
                if tbl not in self.table_placements:
                    # This is a CTE.
                    continue
                self.table_placements[tbl] |= EngineBitmapValues[Engine.Aurora]
                referenced_tables.add(tbl)

        # Update the table movement score if needed.
        for tbl in referenced_tables:
            cur = ctx.current_blueprint.table_locations_bitmap()[tbl]
            nxt = self.table_placements[tbl]
            if ((~cur) & nxt) == 0:
                continue

            result = compute_single_table_movement_time_and_cost(
                tbl, cur, nxt, ctx.current_workload, ctx.planner_config
            )
            self.table_movement_trans_cost += result.movement_cost
            self.table_movement_trans_time_s += result.movement_time_s

    def recompute_provisioning_dependent_scoring(self, ctx: ScoringContext) -> None:
        self._memoized.clear()
        aurora_prov_cost = compute_aurora_hourly_operational_cost(
            self.aurora_provisioning
        )
        redshift_prov_cost = compute_redshift_hourly_operational_cost(
            self.redshift_provisioning
        )

        aurora_transition_time_s = compute_aurora_transition_time_s(
            ctx.current_blueprint.aurora_provisioning(),
            self.aurora_provisioning,
            ctx.planner_config,
        )
        redshift_transition_time_s = compute_redshift_transition_time_s(
            ctx.current_blueprint.redshift_provisioning(),
            self.redshift_provisioning,
            ctx.planner_config,
        )

        self.provisioning_cost = aurora_prov_cost + redshift_prov_cost
        self.provisioning_trans_time_s = (
            aurora_transition_time_s + redshift_transition_time_s
        )

        self.scaled_query_latencies.clear()

        if self.aurora_provisioning.num_nodes() > 0:
            aurora_base = np.array(self.base_query_latencies[Engine.Aurora])
            aurora_predicted = (
                aurora_base
                * ctx.planner_config.aurora_gamma()
                * ctx.planner_config.aurora_alpha()
                * _AURORA_BASE_RESOURCE_VALUE
                / aurora_resource_value(self.aurora_provisioning)
            ) + (aurora_base * (1.0 - ctx.planner_config.aurora_gamma()))
            self.scaled_query_latencies[Engine.Aurora] = aurora_predicted
        else:
            self.scaled_query_latencies[Engine.Aurora] = np.full(
                (len(self.base_query_latencies[Engine.Aurora]),), np.inf
            )

        if self.redshift_provisioning.num_nodes() > 0:
            redshift_base = np.array(self.base_query_latencies[Engine.Redshift])
            redshift_predicted = (
                redshift_base
                * ctx.planner_config.redshift_gamma()
                * ctx.planner_config.redshift_alpha()
                * _REDSHIFT_BASE_RESOURCE_VALUE
                / redshift_resource_value(self.redshift_provisioning)
            ) + (redshift_base * (1.0 - ctx.planner_config.redshift_gamma()))
            self.scaled_query_latencies[Engine.Redshift] = redshift_predicted
        else:
            self.scaled_query_latencies[Engine.Redshift] = np.full(
                (len(self.base_query_latencies[Engine.Redshift]),), np.inf
            )

    def is_better_than(self, other: "_BlueprintCandidate") -> bool:
        return self._comparator(self, other)

    def __lt__(self, other: "_BlueprintCandidate") -> bool:
        # This is implemented for use with Python's `heapq` module. It
        # implements a min-heap, but we want a max-heap.
        return not self.is_better_than(other)

    def find_best_provisioning(self, ctx: ScoringContext) -> None:
        # Tries all provisioinings in the neighborhood and finds the best
        # scoring one for the current table placement and routing.

        if self.explored_provisionings:
            # Already ran before.
            return

        aurora_enumerator = ProvisioningEnumerator(Engine.Aurora)
        aurora_it = aurora_enumerator.enumerate_nearby(
            ctx.current_blueprint.aurora_provisioning(),
            aurora_enumerator.scaling_to_distance(
                ctx.current_blueprint.aurora_provisioning(),
                ctx.planner_config.max_provisioning_multiplier(),
            ),
        )

        redshift_enumerator = ProvisioningEnumerator(Engine.Redshift)
        redshift_it = redshift_enumerator.enumerate_nearby(
            ctx.current_blueprint.redshift_provisioning(),
            redshift_enumerator.scaling_to_distance(
                ctx.current_blueprint.redshift_provisioning(),
                ctx.planner_config.max_provisioning_multiplier(),
            ),
        )

        working_candidate = self.clone()
        current_best = None

        for aurora in aurora_it:
            working_candidate.update_aurora_provisioning(aurora)

            for redshift in redshift_it:
                working_candidate.update_redshift_provisioning(redshift)
                working_candidate.check_feasibility()
                if working_candidate.feasibility == _BlueprintFeasibility.Infeasible:
                    continue

                working_candidate.recompute_provisioning_dependent_scoring(ctx)

                if current_best is None:
                    current_best = working_candidate
                    working_candidate = working_candidate.clone()

                elif working_candidate.is_better_than(current_best):
                    current_best, working_candidate = working_candidate, current_best

        if (
            current_best is None
            or current_best.feasibility == _BlueprintFeasibility.Infeasible
        ):
            self.feasibility = _BlueprintFeasibility.Infeasible
            self.explored_provisionings = True
            return

        self.update_aurora_provisioning(current_best.aurora_provisioning)
        self.update_redshift_provisioning(current_best.redshift_provisioning)
        self.provisioning_cost = current_best.provisioning_cost
        self.provisioning_trans_time_s = current_best.provisioning_trans_time_s

        self.feasibility = current_best.feasibility
        self.explored_provisionings = True

    def check_feasibility(self) -> None:
        # This method checks structural feasibility only (not user-definied
        # feasibility (e.g., it does not check for all analytical queries
        # running under X seconds)).

        if self.feasibility != _BlueprintFeasibility.Unchecked:
            # Already checked.
            return

        if (
            len(self.base_query_latencies[Engine.Aurora]) > 0
            and self.aurora_provisioning.num_nodes() == 0
        ):
            self.feasibility = _BlueprintFeasibility.Infeasible
            return

        if (
            len(self.base_query_latencies[Engine.Redshift]) > 0
            and self.redshift_provisioning.num_nodes() == 0
        ):
            self.feasibility = _BlueprintFeasibility.Infeasible
            return

        # Make sure the provisioning supports the table placement.
        total_bitmap = 0
        for location_bitmap in self.table_placements.values():
            total_bitmap |= location_bitmap

        if (
            (EngineBitmapValues[Engine.Aurora] & total_bitmap) != 0
        ) and self.aurora_provisioning.num_nodes() == 0:
            self.feasibility = _BlueprintFeasibility.Infeasible
            return

        if (
            (EngineBitmapValues[Engine.Redshift] & total_bitmap) != 0
        ) and self.redshift_provisioning.num_nodes() == 0:
            self.feasibility = _BlueprintFeasibility.Infeasible
            return

        self.feasibility = _BlueprintFeasibility.Feasible

    def update_aurora_provisioning(self, prov: Provisioning) -> None:
        self.aurora_provisioning.set_instance_type(prov.instance_type())
        self.aurora_provisioning.set_num_nodes(prov.num_nodes())
        self.feasibility = _BlueprintFeasibility.Unchecked
        self._memoized.clear()

    def update_redshift_provisioning(self, prov: Provisioning) -> None:
        self.redshift_provisioning.set_instance_type(prov.instance_type())
        self.redshift_provisioning.set_num_nodes(prov.num_nodes())
        self.feasibility = _BlueprintFeasibility.Unchecked
        self._memoized.clear()

    def clone(self) -> "_BlueprintCandidate":
        cloned = _BlueprintCandidate(
            self._source_blueprint,
            self.aurora_provisioning.mutable_clone(),
            self.redshift_provisioning.mutable_clone(),
            self.table_placements.copy(),
            self._comparator,
        )

        for engine, indices in self.query_locations.items():
            cloned.query_locations[engine].extend(indices)
        for engine, lats in self.base_query_latencies.items():
            cloned.base_query_latencies[engine].extend(lats)

        cloned.provisioning_cost = self.provisioning_cost
        cloned.storage_cost = self.storage_cost
        cloned.workload_scan_cost = self.workload_scan_cost
        cloned.table_movement_trans_cost = self.table_movement_trans_cost

        cloned.table_movement_trans_time_s = self.table_movement_trans_time_s
        cloned.provisioning_trans_time_s = self.provisioning_trans_time_s

        return cloned

    # `ComparableBlueprint` methods follow.

    def get_table_placement(self) -> Dict[str, List[Engine]]:
        placements = {}
        for name, bitmap in self.table_placements.items():
            placements[name] = Engine.from_bitmap(bitmap)
        return placements

    def get_aurora_provisioning(self) -> Provisioning:
        return self.aurora_provisioning

    def get_redshift_provisioning(self) -> Provisioning:
        return self.redshift_provisioning

    def get_predicted_analytical_latencies(self) -> npt.NDArray:
        relevant = []
        relevant.append(self.scaled_query_latencies[Engine.Aurora])
        relevant.append(self.scaled_query_latencies[Engine.Redshift])
        relevant.append(np.array(self.base_query_latencies[Engine.Athena]))
        return np.concatenate(relevant)

    def get_operational_monetary_cost(self) -> float:
        return self.storage_cost + self.provisioning_cost + self.workload_scan_cost

    def get_transition_cost(self) -> float:
        return self.table_movement_trans_cost

    def get_transition_time_s(self) -> float:
        return self.table_movement_trans_time_s + self.provisioning_trans_time_s

    def set_memoized_value(self, key: str, value: Any) -> None:
        self._memoized[key] = value

    def get_memoized_value(self, key: str) -> Optional[Any]:
        try:
            return self._memoized[key]
        except KeyError:
            return None


_AURORA_BASE_RESOURCE_VALUE = aurora_resource_value(Provisioning("db.r6i.large", 1))
_REDSHIFT_BASE_RESOURCE_VALUE = redshift_resource_value(Provisioning("dc2.large", 1))
