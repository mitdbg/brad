import asyncio
import heapq
import json
import logging
from typing import List

from brad.config.engine import Engine, EngineBitmapValues
from brad.planner import BlueprintPlanner
from brad.planner.beam.feasibility import BlueprintFeasibility
from brad.planner.beam.query_based_candidate import BlueprintCandidate
from brad.planner.debug_logger import BlueprintPlanningDebugLogger
from brad.planner.enumeration.provisioning import ProvisioningEnumerator
from brad.planner.scoring.context import ScoringContext
from brad.routing.rule_based import RuleBased
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
        self._analytics_latency_scorer.apply_predicted_latencies(self._current_workload)

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
            else:
                logger.debug("Skipping table sizing because sizes are already present.")

            ctx = ScoringContext(
                self._current_blueprint,
                self._current_workload,
                next_workload,
                self._metrics_provider.get_metrics(),
                self._planner_config,
            )
            ctx.simulate_current_workload_routing(
                RuleBased(
                    table_placement_bitmap=self._current_blueprint.table_locations_bitmap()
                )
            )
            ctx.compute_engine_latency_weights()

            # 5. Initialize planning state.
            beam_size = self._planner_config.beam_size()
            engines = [Engine.Aurora, Engine.Redshift, Engine.Athena]
            first_query_idx = query_indices[0]
            current_top_k: List[BlueprintCandidate] = []

            # Not a fundamental limitation, but it simplifies the implementation
            # below if this condition is true.
            assert beam_size >= len(engines)

            # 6. Initialize the top-k set (beam).
            for routing_engine in engines:
                candidate = BlueprintCandidate.based_on(
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

                if candidate.feasibility == BlueprintFeasibility.Infeasible:
                    candidate.find_best_provisioning(ctx)
                if candidate.feasibility == BlueprintFeasibility.Infeasible:
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

                next_top_k: List[BlueprintCandidate] = []
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
                            == BlueprintFeasibility.Infeasible
                        ):
                            next_candidate.find_best_provisioning(ctx)
                        if (
                            next_candidate.feasibility
                            == BlueprintFeasibility.Infeasible
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
            final_top_k: List[BlueprintCandidate] = []

            aurora_enumerator = ProvisioningEnumerator(Engine.Aurora)
            redshift_enumerator = ProvisioningEnumerator(Engine.Redshift)

            for candidate in current_top_k:
                aurora_it = aurora_enumerator.enumerate_nearby(
                    ctx.current_blueprint.aurora_provisioning(),
                    aurora_enumerator.scaling_to_distance(
                        ctx.current_blueprint.aurora_provisioning(),
                        ctx.planner_config.max_provisioning_multiplier(),
                    ),
                )
                for aurora in aurora_it:
                    redshift_it = redshift_enumerator.enumerate_nearby(
                        ctx.current_blueprint.redshift_provisioning(),
                        redshift_enumerator.scaling_to_distance(
                            ctx.current_blueprint.redshift_provisioning(),
                            ctx.planner_config.max_provisioning_multiplier(),
                        ),
                    )
                    for redshift in redshift_it:
                        new_candidate = candidate.clone()
                        new_candidate.update_aurora_provisioning(aurora)
                        new_candidate.update_redshift_provisioning(redshift)
                        new_candidate.check_feasibility()
                        if new_candidate.feasibility == BlueprintFeasibility.Infeasible:
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
            logger.debug(
                "Selected blueprint details: %s", json.dumps(debug_values, indent=2)
            )

        finally:
            engine_connections.close_sync()
