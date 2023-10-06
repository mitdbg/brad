import heapq
import itertools
import json
import logging
from datetime import timedelta
from typing import Iterable, List, Tuple, Dict

from brad.config.engine import Engine, EngineBitmapValues
from brad.planner.abstract import BlueprintPlanner
from brad.planner.beam.feasibility import BlueprintFeasibility
from brad.planner.beam.triggers import get_beam_triggers
from brad.planner.router_provider import RouterProvider
from brad.planner.beam.table_based_candidate import BlueprintCandidate
from brad.planner.debug_logger import BlueprintPlanningDebugLogger
from brad.planner.enumeration.provisioning import ProvisioningEnumerator
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.table_placement import compute_single_athena_table_cost
from brad.planner.triggers.trigger import Trigger
from brad.planner.workload import Workload

logger = logging.getLogger(__name__)


class TableBasedBeamPlanner(BlueprintPlanner):
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

    def _check_if_metrics_warrant_replanning(self) -> bool:
        # See if the metrics indicate that we should trigger the planning
        # process.
        return False

    async def _run_replan_impl(self, window_multiplier: int = 1) -> None:
        logger.info("Running a replan...")

        # 1. Fetch metrics and the next workload and then apply predictions.
        metrics, metrics_timestamp = self._metrics_provider.get_metrics()
        current_workload, next_workload = self._workload_provider.get_workloads(
            metrics_timestamp, window_multiplier, desired_period=timedelta(hours=1)
        )
        self._analytics_latency_scorer.apply_predicted_latencies(next_workload)
        self._analytics_latency_scorer.apply_predicted_latencies(current_workload)
        self._data_access_provider.apply_access_statistics(next_workload)
        self._data_access_provider.apply_access_statistics(current_workload)

        # 2. Cluster queries by tables and sort by gains (sum).
        clusters = self._preprocess_workload_queries(next_workload)

        # Sanity check. We cannot run planning without at least one query in the
        # workload.
        assert len(clusters) > 0
        if len(clusters) == 0:
            logger.info("No queries in the workload. Cannot replan.")
            return

        if len(next_workload.analytical_queries()) < 20:
            logger.info("[Table-Based Planner] Query arrival counts")
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
                self._current_blueprint.table_locations_bitmap(),
            )
        )
        ctx.compute_engine_latency_weights()

        beam_size = self._planner_config.beam_size()
        placement_options = self._get_table_placement_options_bitmap()
        first_cluster = clusters[0]
        current_top_k: List[BlueprintCandidate] = []

        # Not a fundamental limitation, but it simplifies the implementation
        # below if this condition is true.
        assert beam_size >= len(placement_options)

        # 4. Initialize the top-k set (beam).
        for placement_bitmap in placement_options:
            candidate = BlueprintCandidate.based_on(
                self._current_blueprint, self._comparator
            )
            candidate.add_transactional_tables(ctx)
            tables, queries, _ = first_cluster
            placement_changed = candidate.add_placement(placement_bitmap, tables, ctx)
            await candidate.add_query_cluster(
                self._router_provider,
                queries,
                reroute_prev=placement_changed,
                ctx=ctx,
            )

            candidate.try_to_make_feasible_if_needed(ctx)
            if candidate.feasibility == BlueprintFeasibility.Infeasible:
                continue

            current_top_k.append(candidate)

        if len(current_top_k) == 0:
            logger.error(
                "Table-based beam blueprint planning failed. Could not generate an initial set of feasible blueprints."
            )
            return

        # 5. Run beam search to formulate the rest of the table placements.
        for j, cluster in enumerate(clusters[1:]):
            if j % 100 == 0:
                logger.debug("Processing index %d of %d", j, len(clusters[1:]))

            next_top_k: List[BlueprintCandidate] = []
            tables, queries, _ = cluster

            # For each candidate in the current top k, expand it based on
            # table placement changes.
            for curr_candidate in current_top_k:
                # It's possible that we build a table placement that is
                # identical to the one in `curr_candidate`. In that case, we
                # just need to add the new query cluster to the blueprint
                # (for scoring purposes). We use this flag to avoid
                # revisiting the same placement again.
                already_processed_identical = False

                for placement_bitmap in placement_options:
                    next_candidate = curr_candidate.clone()
                    placement_changed = next_candidate.add_placement(
                        placement_bitmap, tables, ctx
                    )

                    if not placement_changed and already_processed_identical:
                        # We already examined a blueprint with the same
                        # table placement that includes this query cluster.
                        continue

                    await next_candidate.add_query_cluster(
                        self._router_provider,
                        queries,
                        reroute_prev=placement_changed,
                        ctx=ctx,
                    )

                    if not placement_changed:
                        # Avoid revisiting blueprints with identical table
                        # placements that include this query cluster.
                        already_processed_identical = True

                    next_candidate.try_to_make_feasible_if_needed(ctx)
                    if next_candidate.feasibility == BlueprintFeasibility.Infeasible:
                        continue

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
            self._config, "brad_table_beam_placement_topk"
        )
        if placement_top_k_logger is not None:
            for candidate in current_top_k:
                placement_top_k_logger.log_debug_values(candidate.to_debug_values())

        # 6. Run a final greedy search over provisionings in the top-k set.
        final_top_k: List[BlueprintCandidate] = []

        aurora_enumerator = ProvisioningEnumerator(Engine.Aurora)
        redshift_enumerator = ProvisioningEnumerator(Engine.Redshift)

        for candidate in current_top_k:
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
                        heapq.heappushpop(final_top_k, new_candidate)

        # Best blueprint will be ordered first (we have a negated `__lt__`
        # method to work with `heapq` to create a max heap).
        final_top_k.sort(reverse=True)

        if len(final_top_k) == 0:
            logger.error(
                "The table-based beam planner failed to find any feasible blueprints."
            )
            return

        # Log the final top k for debugging purposes, if needed.
        final_top_k_logger = BlueprintPlanningDebugLogger.create_if_requested(
            self._config, "brad_table_beam_final_topk"
        )
        if final_top_k_logger is not None:
            for candidate in final_top_k:
                final_top_k_logger.log_debug_values(candidate.to_debug_values())

        best_candidate = final_top_k[0]

        # 7. Touch up the table placements. Add any missing tables to ensure
        #    we do not have data loss.
        for tbl, placement_bitmap in best_candidate.table_placements.items():
            if placement_bitmap != 0:
                continue
            # Put the table on Athena (this is a heuristic: we assume the
            # table is rarely accessed).
            best_candidate.table_placements[tbl] |= EngineBitmapValues[Engine.Athena]
            best_candidate.storage_cost += compute_single_athena_table_cost(tbl, ctx)

        # 8. Output the new blueprint.
        best_blueprint = best_candidate.to_blueprint()
        best_blueprint_score = best_candidate.to_score()
        self._last_suggested_blueprint = best_blueprint
        self._last_suggested_blueprint_score = best_blueprint_score

        logger.info("Selected blueprint:")
        logger.info("%s", best_blueprint)

        debug_values = best_candidate.to_debug_values()
        logger.info(
            "Selected blueprint details: %s",
            json.dumps(debug_values, indent=2, default=str),
        )
        logger.info(
            "Metrics used during planning: %s", json.dumps(metrics._asdict(), indent=2)
        )

        await self._notify_new_blueprint(best_blueprint, best_blueprint_score)

    def _preprocess_workload_queries(
        self, workload: Workload
    ) -> List[Tuple[Tuple[str, ...], List[int], float]]:
        query_gains = workload.compute_latency_gains()
        analytical_queries = workload.analytical_queries()

        # Cluster queries by tables referenced.
        clusters_map: Dict[Tuple[str, ...], List[int]] = {}
        existing_table_locs = self._current_blueprint.table_locations()

        for query_idx, q in enumerate(analytical_queries):
            tables = []
            for t in q.tables():
                if t not in existing_table_locs:
                    # Likely a CTE.
                    continue
                tables.append(t)
            tables.sort()
            tbl_key = tuple(tables)
            if tbl_key not in clusters_map:
                clusters_map[tbl_key] = []
            clusters_map[tbl_key].append(query_idx)

        clusters_list = []
        for table_list, query_indices in clusters_map.items():
            total_gain = query_gains[query_indices].sum()
            clusters_list.append((table_list, query_indices, total_gain))

        clusters_list.sort(key=lambda cluster: cluster[2], reverse=True)
        return clusters_list

    def _get_table_placement_options_bitmap(self) -> List[int]:
        engines = [Engine.Aurora, Engine.Redshift, Engine.Athena]

        placement_bitmaps = []
        for num_tbls in range(1, len(engines) + 1):
            for placement_indices in itertools.combinations(
                range(len(engines)), num_tbls
            ):
                bitmap = 0
                for idx in placement_indices:
                    bitmap |= EngineBitmapValues[engines[idx]]
                placement_bitmaps.append(bitmap)

        # N.B. Because of how we set up the engine bit masks, this should be an
        # list with the values [1, 2, ..., 7]. But this function works with
        # general bit masks.
        return placement_bitmaps
