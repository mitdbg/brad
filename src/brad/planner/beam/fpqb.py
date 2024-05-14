import asyncio
import heapq
import json
import logging
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta, datetime
from typing import List, Tuple, Optional
from itertools import product

from brad.blueprint.blueprint import Blueprint
from brad.blueprint.provisioning import Provisioning
from brad.config.engine import Engine, EngineBitmapValues
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.planner.abstract import BlueprintPlanner
from brad.planner.beam.fpqb_candidate import BlueprintCandidate
from brad.planner.compare.function import BlueprintComparator
from brad.planner.compare.provider import BlueprintComparatorProvider
from brad.planner.debug_logger import BlueprintPickleDebugLogger
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
from brad.planner.triggers.provider import EmptyTriggerProvider
from brad.planner.workload import Workload
from brad.planner.workload.provider import WorkloadProvider
from brad.routing.router import Router


logger = logging.getLogger(__name__)


class FixedProvisioningQueryBasedBeamPlanner(BlueprintPlanner):
    def __init__(
        self,
        *args,
        disable_external_logging: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._disable_external_logging = disable_external_logging

    async def _run_replan_impl(
        self, window_multiplier: int = 1, beam_override: int = 1
    ) -> Optional[Tuple[Blueprint, Score]]:
        logger.info("Running a fixed provisioning query-based beam replan...")
        logger.info("Using beam override %d", beam_override)

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

        if self._planner_config.flag("ensure_tables_together_on_one_engine"):
            # This adds a constraint to ensure all tables are present together
            # on at least one engine. This ensures that arbitrary unseen join
            # templates can always be immediately handled.
            all_tables = ", ".join(
                [
                    table.name
                    for table in self._current_blueprint.tables()
                    if table.name != "embeddings"
                ]
            )
            next_workload.add_priming_analytical_query(
                f"SELECT 1 FROM {all_tables} LIMIT 1"
            )

        # If requested, we record this planning pass for later debugging.
        if (
            not self._disable_external_logging
            and BlueprintPickleDebugLogger.is_log_requested(self._config)
        ):
            planning_run = RecordedFpqbPlanningRun(
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
                self._config, "fpqb_run", planning_run
            )

        # 2. Compute query gains and reorder queries by their gain in descending
        # order.
        gains = next_workload.compute_latency_gains()
        query_indices = list(range(len(next_workload.analytical_queries())))

        # Want to process queries in decreasing order of frequency followed by
        # predicted cross-engine gains. Python's sort is stable, so we just
        # perform two sorts (first by decreasing gain, then arrival frequency).
        query_indices.sort(key=lambda idx: gains[idx], reverse=True)
        overall_arrival_counts = next_workload.get_arrival_counts()
        query_indices.sort(key=lambda idx: overall_arrival_counts[idx], reverse=True)

        # Sanity check. We cannot run planning without at least one query in the
        # workload.
        if len(query_indices) == 0:
            logger.info("No queries in the workload. Cannot replan.")
            return None

        # 3. Initialize planning state.
        ctx = ScoringContext(
            self._schema_name,
            self._current_blueprint,
            current_workload,
            next_workload,
            metrics,
            self._planner_config,
        )
        planning_router = Router.create_from_blueprint(self._current_blueprint)
        await planning_router.run_setup_for_standalone(
            self._providers.estimator_provider.get_estimator()
        )
        await ctx.simulate_current_workload_routing(planning_router)
        if not ctx.planner_config.flag("skip_observation_prediction_correction"):
            ctx.correct_predictions_based_on_observations()
        ctx.compute_workload_provisioning_predictions()
        ctx.compute_engine_latency_norm_factor()
        ctx.compute_current_workload_predicted_hourly_scan_cost()
        ctx.compute_current_blueprint_provisioning_hourly_cost()
        ctx.compute_table_transitions()

        # 4. Do the beam search per provisioning.
        aurora_options = []
        aurora_enumerator = ProvisioningEnumerator(Engine.Aurora)
        aurora_it = aurora_enumerator.enumerate_nearby(
            ctx.current_blueprint.aurora_provisioning(),
            ctx.planner_config.aurora_provisioning_search_distance(),
        )
        for aurora in aurora_it:
            aurora_options.append(aurora.clone())

        redshift_options = []
        redshift_enumerator = ProvisioningEnumerator(Engine.Redshift)
        redshift_it = redshift_enumerator.enumerate_nearby(
            ctx.current_blueprint.redshift_provisioning(),
            ctx.planner_config.redshift_provisioning_search_distance(),
        )
        for redshift in redshift_it:
            redshift_options.append(redshift.clone())

        all_provs = list(product(aurora_options, redshift_options))
        if ctx.planner_config.flag("use_sequential_fpqb_search"):
            candidates = await self._do_sequential_search(
                all_provs, query_indices, planning_router, ctx, beam_override
            )
        else:
            max_workers = self._planner_config.planner_max_workers()
            logger.info("Planner running with %d max workers.", max_workers)
            candidates = await self._do_parallel_batched_search(
                all_provs,
                query_indices,
                ctx,
                batch_size=20,
                max_workers=max_workers,
                beam_size=beam_override,
            )
        candidates.sort(reverse=True)
        if len(candidates) == 0:
            logger.error(
                "Fixed provisioning query-based beam blueprint planning failed. "
                "No feasible candidates."
            )
            return None

        # 5. Touch up the table placements. Add any missing tables to ensure
        #    we do not have data loss.
        best_candidate = candidates[0]
        for tbl, placement_bitmap in best_candidate.table_placements.items():
            if placement_bitmap != 0:
                continue
            # Put the table on Athena (this is a heuristic: we assume the
            # table is rarely accessed).
            best_candidate.table_placements[tbl] |= EngineBitmapValues[Engine.Athena]
        best_candidate.compute_score(ctx)

        # 6. Output the new blueprint.
        best_blueprint = best_candidate.to_blueprint(ctx)
        best_blueprint_score = best_candidate.score

        logger.info("Selected blueprint:")
        logger.info("%s", best_blueprint)
        debug_values = best_candidate.to_debug_values()
        logger.info(
            "Selected blueprint details: %s", json.dumps(debug_values, indent=2)
        )
        logger.info(
            "Metrics used during planning: %s",
            json.dumps(metrics._asdict(), indent=2, default=str),
        )

        if not self._disable_external_logging:
            ctx.current_workload.clear_cached()
            ctx.next_workload.clear_cached()
            # For later interactive inspection in Python.
            BlueprintPickleDebugLogger.log_object_if_requested(
                self._config, "final_fpqb_chosen_blueprint", [best_candidate]
            )
            BlueprintPickleDebugLogger.log_object_if_requested(
                self._config, "scoring_context", ctx
            )

        return best_blueprint, best_blueprint_score, debug_values

    async def _do_sequential_search(
        self,
        provisionings: List[Tuple[Provisioning, Provisioning]],
        query_order: List[int],
        planning_router: Router,
        ctx: ScoringContext,
        beam_size: int,
    ) -> List[BlueprintCandidate]:
        comparator = self._providers.comparator_provider.get_comparator(
            ctx.metrics,
            curr_hourly_cost=(
                ctx.current_workload_predicted_hourly_scan_cost
                + ctx.current_blueprint_provisioning_hourly_cost
            ),
        )
        candidates = []
        for j, (aurora, redshift) in enumerate(provisionings):
            if j % 10 == 0:
                logger.debug("Processing provisioning %d of %d", j, len(provisionings))
            candidate = await self._do_query_beam_search(
                aurora,
                redshift,
                comparator,
                query_order,
                planning_router,
                ctx,
                beam_size,
            )
            if candidate is None:
                continue

            # We only need to store the best performing candidate. But it's
            # useful to keep them all around for debugging purposes later on.
            candidates.append(candidate)

        candidates.sort(reverse=True)
        return candidates

    async def _do_parallel_batched_search(
        self,
        provisionings: List[Tuple[Provisioning, Provisioning]],
        query_order: List[int],
        ctx: ScoringContext,
        batch_size: int,
        max_workers: int,
        beam_size: int,
    ) -> List[BlueprintCandidate]:
        loop = asyncio.get_running_loop()
        ppe = ProcessPoolExecutor(max_workers=max_workers)
        futures = []

        num_batches = len(provisionings) // batch_size
        if len(provisionings) % batch_size != 0:
            num_batches += 1

        offset = 0
        while offset < len(provisionings):
            awaitable = loop.run_in_executor(
                ppe,
                self._run_batched_query_beam_search,
                provisionings[offset : offset + batch_size],
                self._providers.comparator_provider,
                query_order,
                ctx,
                beam_size,
            )
            futures.append(asyncio.ensure_future(awaitable))
            offset += batch_size

        progress = asyncio.create_task(log_planning_progress(futures))
        results = await asyncio.gather(progress, *futures)

        # Flatten the results.
        candidates = []
        for result in results:
            if result is None:
                continue
            if not isinstance(result, list):
                continue
            for bpc in result:
                if bpc is None:
                    continue
                candidates.append(bpc)

        if len(candidates) == 0:
            return candidates

        # The candidates go through a serialization boundary and comparators are
        # not serialized. This restores the comparator.
        comparator = self._providers.comparator_provider.get_comparator(
            ctx.metrics,
            curr_hourly_cost=(
                ctx.current_workload_predicted_hourly_scan_cost
                + ctx.current_blueprint_provisioning_hourly_cost
            ),
        )
        for bpc in candidates:
            bpc._comparator = comparator  # pylint: disable=protected-access
        candidates.sort(reverse=True)
        return candidates

    @staticmethod
    def _run_batched_query_beam_search(
        provs: List[Tuple[Provisioning, Provisioning]],
        comparator_provider: BlueprintComparatorProvider,
        query_order: List[int],
        ctx: ScoringContext,
        beam_size: int,
    ) -> List[Optional[BlueprintCandidate]]:
        """
        This is used when running a parallel search. Not meant to be called
        directly.
        """
        comparator = comparator_provider.get_comparator(
            ctx.metrics,
            curr_hourly_cost=(
                ctx.current_workload_predicted_hourly_scan_cost
                + ctx.current_blueprint_provisioning_hourly_cost
            ),
        )
        planning_router = Router.create_from_blueprint(ctx.current_blueprint)
        asyncio.run(planning_router.run_setup_for_standalone(estimator=None))
        results = []
        for aurora, redshift in provs:
            results.append(
                asyncio.run(
                    FixedProvisioningQueryBasedBeamPlanner._do_query_beam_search(
                        aurora,
                        redshift,
                        comparator,
                        query_order,
                        planning_router,
                        ctx,
                        beam_size,
                    )
                )
            )
        return results

    @staticmethod
    async def _do_query_beam_search(
        aurora: Provisioning,
        redshift: Provisioning,
        comparator: BlueprintComparator,
        query_order: List[int],
        planning_router: Router,
        ctx: ScoringContext,
        beam_size: int,
    ) -> Optional[BlueprintCandidate]:
        """
        Performs a query based beam search on the given provisioning. Returns
        the best blueprint candidate. If this returns `None`, it indicates there
        are no feasible candidates for this provisioning.
        """
        current_top_k: List[BlueprintCandidate] = []
        analytical_queries = ctx.next_workload.analytical_queries()

        first_query_idx = query_order[0]
        first_query = analytical_queries[first_query_idx]

        # Initialize.
        for routing_engine in Engine.from_bitmap(
            planning_router.run_functionality_routing(first_query)
        ):
            candidate = BlueprintCandidate.based_on(ctx.current_blueprint, comparator)
            candidate.aurora_provisioning = aurora
            candidate.redshift_provisioning = redshift
            candidate.add_transactional_tables(ctx)
            candidate.add_query(first_query_idx, first_query, routing_engine)
            if not candidate.is_structurally_feasible():
                continue
            candidate.compute_score(ctx)
            current_top_k.append(candidate)

        # Stop early if possible.
        if len(current_top_k) == 0:
            # Indicates that this provisioning is not feasible.
            return None

        # Beam search.
        for j, query_idx in enumerate(query_order[1:]):
            if j % 2 == 0:
                # Yield to allow other tasks to run if they are waiting.
                await asyncio.sleep(0)

            next_top_k: List[BlueprintCandidate] = []
            query = analytical_queries[query_idx]

            # Only a subset of the engines may support this query if it uses
            # "special functionality".
            engine_candidates = Engine.from_bitmap(
                planning_router.run_functionality_routing(query)
            )

            for curr_candidate in current_top_k:
                for routing_engine in engine_candidates:
                    next_candidate = curr_candidate.clone()
                    next_candidate.add_query(query_idx, query, routing_engine)
                    if not next_candidate.is_structurally_feasible():
                        continue
                    next_candidate.compute_score(ctx)

                    if len(next_top_k) < beam_size:
                        next_top_k.append(next_candidate)
                        if len(next_top_k) == beam_size:
                            heapq.heapify(next_top_k)
                    else:
                        if next_candidate.is_better_than(next_top_k[0]):
                            # This next candidate is better than the worst
                            # candidate in the top k. So we add it to the top k.
                            heapq.heappushpop(next_top_k, next_candidate)
                        else:
                            # This next candidate is eliminated.
                            pass

            if len(next_top_k) == 0:
                # Stop early - it's not possible to route the next query
                # without violating constraints.
                return None

            current_top_k = next_top_k

        return max(current_top_k)


async def log_planning_progress(futures: List[asyncio.Future]) -> None:
    completed_tasks = 0
    total_tasks = len(futures)
    pending = set(futures)

    logger.info("Parallel search started. Total batches: %d", len(futures))
    while completed_tasks < total_tasks:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        completed_tasks += len(done)
        logger.info("%d/%d batches completed", completed_tasks, total_tasks)


class RecordedFpqbPlanningRun(RecordedPlanningRun, WorkloadProvider):
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

    def create_planner(self, estimator_provider: EstimatorProvider) -> BlueprintPlanner:
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
        return FixedProvisioningQueryBasedBeamPlanner(
            self._config,
            self._planner_config,
            self._schema_name,
            self._current_blueprint,
            self._current_blueprint_score,
            providers,
            # N.B. Purposefully set to `None`.
            system_event_logger=None,
            disable_external_logging=True,
        )

    # Provider methods follow.

    async def get_workloads(
        self,
        window_end: datetime,
        window_multiplier: int = 1,
        desired_period: Optional[timedelta] = None,
    ) -> Tuple[Workload, Workload]:
        return self._current_workload, self._next_workload
