import asyncio
import heapq
import json
import logging
from datetime import timedelta, datetime
from typing import List, Tuple, Optional

from brad.blueprint.blueprint import Blueprint
from brad.config.engine import Engine, EngineBitmapValues
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.planner.abstract import BlueprintPlanner
from brad.planner.compare.provider import BlueprintComparatorProvider
from brad.planner.beam.feasibility import BlueprintFeasibility
from brad.planner.beam.query_based_candidate import BlueprintCandidate
from brad.planner.debug_logger import (
    BlueprintPlanningDebugLogger,
    BlueprintPickleDebugLogger,
)
from brad.planner.enumeration.provisioning import ProvisioningEnumerator
from brad.planner.estimator import EstimatorProvider
from brad.planner.metrics import Metrics, FixedMetricsProvider
from brad.planner.providers import BlueprintProviders
from brad.planner.recorded_run import RecordedPlanningRun
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.data_access.provider import NoopDataAccessProvider
from brad.planner.scoring.performance.analytics_latency import (
    NoopAnalyticsLatencyScorer,
)
from brad.planner.scoring.score import Score
from brad.planner.scoring.table_placement import compute_single_athena_table_cost
from brad.planner.triggers.provider import EmptyTriggerProvider
from brad.planner.workload import Workload
from brad.planner.workload.provider import WorkloadProvider
from brad.routing.router import Router


logger = logging.getLogger(__name__)


class QueryBasedBeamPlanner(BlueprintPlanner):
    def __init__(
        self,
        *args,
        disable_external_logging: bool = False,
        other_args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._disable_external_logging = disable_external_logging
        self._other_args = other_args

    async def _run_replan_impl(
        self, window_multiplier: int = 1
    ) -> Optional[Tuple[Blueprint, Score]]:
        logger.info("Running a query-based beam replan...")

        # 1. Fetch the next workload and apply predictions.
        metrics, metrics_timestamp = self._providers.metrics_provider.get_metrics()
        (
            current_workload,
            next_workload,
        ) = await self._providers.workload_provider.get_workloads(
            metrics_timestamp, window_multiplier, desired_period=timedelta(hours=1)
        )
        self._providers.analytics_latency_scorer.apply_predicted_latencies(
            next_workload
        )
        self._providers.analytics_latency_scorer.apply_predicted_latencies(
            current_workload
        )
        self._providers.data_access_provider.apply_access_statistics(next_workload)
        self._providers.data_access_provider.apply_access_statistics(current_workload)

        # if self._planner_config.flag("ensure_tables_together_on_one_engine"):
        #     # This adds a constraint to ensure all tables are present together
        #     # on at least one engine. This ensures that arbitrary unseen join
        #     # templates can always be immediately handled.
        #     all_tables = ", ".join(
        #         [table.name for table in self._current_blueprint.tables()]
        #     )
        #     next_workload.add_priming_analytical_query(
        #         f"SELECT 1 FROM {all_tables} LIMIT 1"
        #     )

        # If requested, we record this planning pass for later debugging.
        if (
            not self._disable_external_logging
            and BlueprintPickleDebugLogger.is_log_requested(self._config)
        ):
            planning_run = RecordedQueryBasedPlanningRun(
                self._config,
                self._planner_config,
                self._schema_name,
                self._current_blueprint,
                self._current_blueprint_score,
                current_workload,
                next_workload,
                metrics,
                metrics_timestamp,
                self._providers.comparator_provider,
            )
            BlueprintPickleDebugLogger.log_object_if_requested(
                self._config, "query_beam_run", planning_run
            )

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
            return None

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
        ctx.set_up_sensitivity_state(self._other_args)
        planning_router = Router.create_from_blueprint(self._current_blueprint)
        await planning_router.run_setup_for_standalone(
            self._providers.estimator_provider.get_estimator()
        )
        await ctx.simulate_current_workload_routing(planning_router)
        ctx.compute_engine_latency_norm_factor()
        ctx.compute_current_workload_predicted_hourly_scan_cost()
        ctx.compute_current_blueprint_provisioning_hourly_cost()

        comparator = self._providers.comparator_provider.get_comparator(
            metrics,
            curr_hourly_cost=(
                ctx.current_workload_predicted_hourly_scan_cost
                + ctx.current_blueprint_provisioning_hourly_cost
            ),
        )
        beam_size = self._planner_config.beam_size()
        first_query_idx = query_indices[0]
        first_query = analytical_queries[first_query_idx]
        current_top_k: List[BlueprintCandidate] = []

        # 4. Initialize the top-k set (beam).
        for routing_engine in Engine.from_bitmap(
            planning_router.run_functionality_routing(first_query)
        ):
            candidate = BlueprintCandidate.based_on(self._current_blueprint, comparator)
            candidate.add_transactional_tables(ctx)
            candidate.add_query(
                first_query_idx,
                first_query,
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
                "Query-based beam blueprint planning failed. "
                "Could not generate an initial set of feasible blueprints."
            )
            return None

        # 5. Run beam search to formulate the table placements.
        for j, query_idx in enumerate(query_indices[1:]):
            if j % 5 == 0:
                # This is a long-running process. We should yield every so often
                # to allow other tasks to run on the daemon (e.g., processing
                # metrics messages).
                await asyncio.sleep(0)

            logger.debug("Processing index %d of %d", j, len(query_indices[1:]))

            next_top_k: List[BlueprintCandidate] = []
            query = analytical_queries[query_idx]

            # Only a subset of the engines may support this query if it uses
            # "special functionality".
            engine_candidates = Engine.from_bitmap(
                planning_router.run_functionality_routing(query)
            )

            # For each candidate in the current top k, expand it by one
            # query in the workload.
            for curr_candidate in current_top_k:
                for routing_engine in engine_candidates:
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
                            # We eliminate `next_candidate`. Even after looking
                            # for the best provisioning, it has a worse score
                            # compared to `next_top_k[0]` (the worst scoring
                            # blueprint candidate in the top-k).
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

        if not self._disable_external_logging:
            # Log the placement top k for debugging purposes, if needed.
            placement_top_k_logger = BlueprintPlanningDebugLogger.create_if_requested(
                self._config, "query_beam_placement_topk"
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

        if len(final_top_k) == 0:
            logger.error(
                "The query-based beam planner failed to find any feasible blueprints."
            )
            return None

        # The best blueprint will be ordered first (we have a negated
        # `__lt__` method to work with `heapq` to create a max heap).
        final_top_k.sort(reverse=True)

        if not self._disable_external_logging:
            # For later interactive inspection in Python.
            BlueprintPickleDebugLogger.log_object_if_requested(
                self._config, "final_query_based_blueprints", final_top_k
            )
            BlueprintPickleDebugLogger.log_object_if_requested(
                self._config, "scoring_context", ctx
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
        comparator(best_candidate, best_candidate)
        best_blueprint = best_candidate.to_blueprint(ctx, use_legacy_behavior=False)
        best_blueprint_score = best_candidate.to_score()

        logger.info("Selected blueprint:")
        logger.info("%s", best_blueprint)
        debug_values = best_candidate.to_debug_values()
        best_blueprint_score.debug_values = debug_values
        logger.info(
            "Selected blueprint details: %s", json.dumps(debug_values, indent=2)
        )
        logger.info(
            "Metrics used during planning: %s",
            json.dumps(metrics._asdict(), indent=2, default=str),
        )

        return best_blueprint, best_blueprint_score


class RecordedQueryBasedPlanningRun(RecordedPlanningRun, WorkloadProvider):
    def __init__(
        self,
        config: ConfigFile,
        planner_config: PlannerConfig,
        schema_name: str,
        current_blueprint: Blueprint,
        current_blueprint_score: Optional[Score],
        current_workload: Workload,
        next_workload: Workload,
        metrics: Metrics,
        metrics_timestamp: datetime,
        comparator_provider: BlueprintComparatorProvider,
    ) -> None:
        self._config = config
        self._planner_config = planner_config
        self._schema_name = schema_name
        self._current_blueprint = current_blueprint
        self._current_blueprint_score = current_blueprint_score
        self._current_workload = current_workload
        self._next_workload = next_workload
        self._metrics = metrics
        self._metrics_timestamp = metrics_timestamp
        self._comparator_provider = comparator_provider

    def create_planner(
        self, estimator_provider: EstimatorProvider, args
    ) -> BlueprintPlanner:
        providers = BlueprintProviders(
            workload_provider=self,
            analytics_latency_scorer=NoopAnalyticsLatencyScorer(),
            comparator_provider=self._comparator_provider,
            metrics_provider=FixedMetricsProvider(
                self._metrics, self._metrics_timestamp
            ),
            data_access_provider=NoopDataAccessProvider(),
            estimator_provider=estimator_provider,
            trigger_provider=EmptyTriggerProvider(),
        )
        return QueryBasedBeamPlanner(
            self._config,
            self._planner_config,
            self._schema_name,
            self._current_blueprint,
            self._current_blueprint_score,
            providers,
            # N.B. Purposefully set to `None`.
            system_event_logger=None,
            disable_external_logging=True,
            other_args=args,
        )

    # Provider methods follow.

    async def get_workloads(
        self,
        window_end: datetime,
        window_multiplier: int = 1,
        desired_period: Optional[timedelta] = None,
    ) -> Tuple[Workload, Workload]:
        return self._current_workload, self._next_workload
