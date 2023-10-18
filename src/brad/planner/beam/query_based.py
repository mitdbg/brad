import heapq
import json
import logging
from datetime import timedelta
from typing import Iterable, List

from brad.config.engine import Engine, EngineBitmapValues
from brad.planner.abstract import BlueprintPlanner
from brad.planner.beam.feasibility import BlueprintFeasibility
from brad.planner.beam.query_based_candidate import BlueprintCandidate
from brad.planner.beam.triggers import get_beam_triggers
from brad.planner.router_provider import RouterProvider
from brad.planner.debug_logger import (
    BlueprintPlanningDebugLogger,
    BlueprintPickleDebugLogger,
)
from brad.planner.enumeration.provisioning import ProvisioningEnumerator
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.table_placement import compute_single_athena_table_cost
from brad.planner.triggers.trigger import Trigger


logger = logging.getLogger(__name__)


class QueryBasedBeamPlanner(BlueprintPlanner):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._router_provider = RouterProvider(
            self._schema_name, self._config, self._estimator_provider
        )
        self._triggers = get_beam_triggers(
            self._config,
            self._planner_config,
            self._monitor,
            self._data_access_provider,
            self._router_provider,
        )
        for t in self._triggers:
            t.update_blueprint(self._current_blueprint, self._current_blueprint_score)

    def get_triggers(self) -> Iterable[Trigger]:
        return self._triggers

    async def _run_replan_impl(self, window_multiplier: int = 1) -> None:
        logger.info("Running a replan...")

        # 1. Fetch the next workload and apply predictions.
        metrics, metrics_timestamp = self._metrics_provider.get_metrics()
        logger.debug("Using metrics: %s", str(metrics))
        current_workload, next_workload = self._workload_provider.get_workloads(
            metrics_timestamp, window_multiplier, desired_period=timedelta(hours=1)
        )
        self._analytics_latency_scorer.apply_predicted_latencies(next_workload)
        self._analytics_latency_scorer.apply_predicted_latencies(current_workload)
        self._data_access_provider.apply_access_statistics(next_workload)
        self._data_access_provider.apply_access_statistics(current_workload)

        # 2. Compute query gains and reorder queries by their gain in descending
        # order.
        gains = next_workload.compute_latency_gains()
        analytical_queries = next_workload.analytical_queries()
        query_indices = list(range(len(next_workload.analytical_queries())))
        query_indices.sort(key=lambda idx: gains[idx], reverse=True)

        # Sanity check. We cannot run planning without at least one query in the
        # workload.
        if len(query_indices) == 0:
            logger.info("No queries in the workload. Cannot replan.")
            return

        if len(next_workload.analytical_queries()) < 20:
            logger.info("[Query-Based Planner] Query arrival counts")
            for q in next_workload.analytical_queries():
                logger.info("Query: %s -- Count: %d", q.raw_query, q.arrival_count())

        # 3. Initialize planning state.
        ctx = ScoringContext(
            self._schema_name,
            self._current_blueprint,
            current_workload,
            next_workload,
            metrics,
            self._planner_config,
        )
        await ctx.simulate_current_workload_routing(
            await self._router_provider.get_router(
                self._current_blueprint.table_locations_bitmap()
            )
        )
        ctx.compute_engine_latency_weights()

        beam_size = self._planner_config.beam_size()
        engines = [Engine.Aurora, Engine.Redshift, Engine.Athena]
        first_query_idx = query_indices[0]
        current_top_k: List[BlueprintCandidate] = []

        # Not a fundamental limitation, but it simplifies the implementation
        # below if this condition is true.
        assert beam_size >= len(engines)

        # 4. Initialize the top-k set (beam).
        for routing_engine in engines:
            candidate = BlueprintCandidate.based_on(
                self._current_blueprint, self._comparator
            )
            candidate.add_transactional_tables(ctx)
            query = analytical_queries[first_query_idx]
            candidate.add_query(
                first_query_idx,
                query,
                routing_engine,
                next_workload.get_predicted_analytical_latency(
                    first_query_idx, routing_engine
                ),
                ctx,
            )
            candidate.try_to_make_feasible_if_needed(ctx)
            if candidate.feasibility == BlueprintFeasibility.Infeasible:
                continue

            current_top_k.append(candidate)

        if len(current_top_k) == 0:
            logger.error(
                "Query-based beam blueprint planning failed. Could not generate an initial set of feasible blueprints."
            )
            return

        # 5. Run beam search to formulate the table placements.
        for j, query_idx in enumerate(query_indices[1:]):
            if j % 100 == 0:
                logger.debug("Processing index %d of %d", j, len(query_indices[1:]))

            next_top_k: List[BlueprintCandidate] = []
            query = analytical_queries[query_idx]

            # For each candidate in the current top k, expand it by one
            # query in the workload.
            for curr_candidate in current_top_k:
                for routing_engine in engines:
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
                    next_candidate.try_to_make_feasible_if_needed(ctx)
                    if next_candidate.feasibility == BlueprintFeasibility.Infeasible:
                        continue

                    # Check if this blueprint is part of the top k. If so,
                    # add it to the next top k.
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
                            if next_candidate.redshift_provisioning.num_nodes() == 0:
                                logger.info("Eliminating BP with turned off Redshift.")
                                logger.info(
                                    "Score: %s",
                                    json.dumps(
                                        next_candidate.to_debug_values(), indent=2
                                    ),
                                )
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
                            removed = heapq.heappushpop(next_top_k, next_candidate)
                            if removed.redshift_provisioning.num_nodes() == 0:
                                logger.info("Eliminating BP with turned off Redshift.")
                                logger.info(
                                    "Score: %s",
                                    json.dumps(removed.to_debug_values(), indent=2),
                                )

            current_top_k = next_top_k

        # Log the placement top k for debugging purposes, if needed.
        placement_top_k_logger = BlueprintPlanningDebugLogger.create_if_requested(
            self._config, "query_beam_placement_topk"
        )
        if placement_top_k_logger is not None:
            for candidate in current_top_k:
                placement_top_k_logger.log_debug_values(candidate.to_debug_values())

        # 8. We generated the placements by placing queries using run time
        #    predictions. Now we re-route the queries using the fixed placements
        #    but with the actual routing policy that we will use at runtime.
        rerouted_top_k: List[BlueprintCandidate] = []

        for candidate in current_top_k:
            query_indices = candidate.get_all_query_indices()
            candidate.reset_routing()
            router = await self._router_provider.get_router(candidate.table_placements)
            for qidx in query_indices:
                query = analytical_queries[qidx]
                routing_engine = await router.engine_for(query)
                candidate.add_query_last_step(
                    qidx,
                    query,
                    routing_engine,
                    next_workload.get_predicted_analytical_latency(
                        qidx, routing_engine
                    ),
                    ctx,
                )

            if not candidate.is_structurally_feasible():
                continue

            rerouted_top_k.append(candidate)

        if len(rerouted_top_k) == 0:
            logger.error(
                "The query-based beam planner failed to find any feasible placements after re-routing the queries."
            )
            return

        # 8. Run a final greedy search over provisionings in the top-k set.
        final_top_k: List[BlueprintCandidate] = []

        aurora_enumerator = ProvisioningEnumerator(Engine.Aurora)
        redshift_enumerator = ProvisioningEnumerator(Engine.Redshift)

        for candidate in rerouted_top_k:
            aurora_it = aurora_enumerator.enumerate_nearby(
                ctx.current_blueprint.aurora_provisioning(),
                aurora_enumerator.scaling_to_distance(
                    ctx.current_blueprint.aurora_provisioning(),
                    ctx.planner_config.max_provisioning_multiplier(),
                    Engine.Aurora,
                ),
            )
            for aurora in aurora_it:
                redshift_it = redshift_enumerator.enumerate_nearby(
                    ctx.current_blueprint.redshift_provisioning(),
                    redshift_enumerator.scaling_to_distance(
                        ctx.current_blueprint.redshift_provisioning(),
                        ctx.planner_config.max_provisioning_multiplier(),
                        Engine.Redshift,
                    ),
                )
                for redshift in redshift_it:
                    new_candidate = candidate.clone()
                    new_candidate.update_aurora_provisioning(aurora)
                    new_candidate.update_redshift_provisioning(redshift)
                    if not new_candidate.is_structurally_feasible():
                        continue

                    new_candidate.recompute_provisioning_dependent_scoring(ctx)
                    new_candidate.compute_runtime_feasibility(ctx)
                    if new_candidate.feasibility == BlueprintFeasibility.Infeasible:
                        continue

                    if len(final_top_k) < beam_size:
                        final_top_k.append(new_candidate)
                        if len(final_top_k) == beam_size:
                            heapq.heapify(final_top_k)
                    elif new_candidate.is_better_than(final_top_k[0]):
                        removed = heapq.heappushpop(final_top_k, new_candidate)
                        if removed.redshift_provisioning.num_nodes() == 0:
                            logger.info(
                                "Eliminating BP with turned off Redshift (2nd step)."
                            )
                            logger.info(
                                "Score: %s",
                                json.dumps(removed.to_debug_values(), indent=2),
                            )

        if len(final_top_k) == 0:
            logger.error(
                "The query-based beam planner failed to find any feasible blueprints."
            )
            return

        # The best blueprint will be ordered first (we have a negated
        # `__lt__` method to work with `heapq` to create a max heap).
        final_top_k.sort(reverse=True)

        # For later interactive inspection in Python.
        BlueprintPickleDebugLogger.log_candidates_if_requested(
            self._config, "final_query_based_blueprints", final_top_k
        )

        # Log the final top k for debugging purposes, if needed.
        final_top_k_logger = BlueprintPlanningDebugLogger.create_if_requested(
            self._config, "query_beam_final_topk"
        )
        if final_top_k_logger is not None:
            for candidate in final_top_k:
                final_top_k_logger.log_debug_values(candidate.to_debug_values())

        best_candidate = final_top_k[0]

        # 8. Touch up the table placements. Add any missing tables to ensure
        #    we do not have data loss.
        for tbl, placement_bitmap in best_candidate.table_placements.items():
            if placement_bitmap != 0:
                continue
            # Put the table on Athena (this is a heuristic: we assume the
            # table is rarely accessed).
            best_candidate.table_placements[tbl] |= EngineBitmapValues[Engine.Athena]
            # We added the table to Athena.
            best_candidate.storage_cost += compute_single_athena_table_cost(tbl, ctx)

        # 9. Output the new blueprint.
        best_blueprint = best_candidate.to_blueprint()
        best_blueprint_score = best_candidate.to_score()
        self._last_suggested_blueprint = best_blueprint
        self._last_suggested_blueprint_score = best_blueprint_score

        logger.info("Selected blueprint:")
        logger.info("%s", best_blueprint)

        debug_values = best_candidate.to_debug_values()
        logger.info(
            "Selected blueprint details: %s", json.dumps(debug_values, indent=2)
        )
        logger.info(
            "Metrics used during planning: %s", json.dumps(metrics._asdict(), indent=2)
        )

        await self._notify_new_blueprint(best_blueprint, best_blueprint_score)
