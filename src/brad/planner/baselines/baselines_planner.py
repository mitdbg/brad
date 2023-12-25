import json
import logging
import random
from datetime import timedelta
from typing import Tuple, Optional

import numpy as np
import numpy.typing as npt
from itertools import product

from brad.blueprint.blueprint import Blueprint
from brad.config.engine import Engine, EngineBitmapValues
from brad.planner.abstract import BlueprintPlanner
from brad.planner.beam.feasibility import BlueprintFeasibility
from brad.planner.beam.query_based_candidate import BlueprintCandidate
from brad.planner.enumeration.provisioning import ProvisioningEnumerator
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.score import Score
from brad.planner.scoring.table_placement import compute_single_athena_table_cost
from brad.planner.workload import Workload
from brad.routing.router import Router


logger = logging.getLogger(__name__)


def provisioning_search(ctx, candidate):
    # Copied from beam search planner:
    # 6. Run a final greedy search over provisionings in the top-k set.
    aurora_enumerator = ProvisioningEnumerator(Engine.Aurora)
    redshift_enumerator = ProvisioningEnumerator(Engine.Redshift)

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

            if new_candidate.is_better_than(candidate):
                candidate = new_candidate

    return candidate

def place_unused_tables(ctx, best_candidate):
    # Copied from beam search planner:
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


class BaselinePlanner(BlueprintPlanner):
    def __init__(
        self,
        *args,
        baseline,
        num_samples=2,
        random_seed=42,
        n_queries,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.baseline = baseline
        self.num_samples = num_samples
        self.prng = random.Random(random_seed)
        self.n_queries = n_queries

    def plan_random(
        self, workload: Workload, planning_router: Router, ctx: ScoringContext
    ) -> Optional[Tuple[Blueprint, Score]]:
        """
        Baseline: Place each query on a random engine.
        Choose table placement and provisioning accordingly.
        """
        best_candidate: Optional[BlueprintCandidate] = None
        for _ in range(self.num_samples):
            # Initialize candidate and put all transactional tables on Aurora.
            candidate = BlueprintCandidate.based_on(
                self._current_blueprint, self._comparator
            )
            candidate.add_transactional_tables(ctx)

            analytical_queries = workload.analytical_queries()[:self.n_queries]
            for query_idx, query in enumerate(analytical_queries):
                # Run functionality routing
                supp_engines = Engine.from_bitmap(
                    planning_router.run_functionality_routing(query)
                )

                # Sample random engine from the supported engines.
                routing_engine = self.prng.choice(supp_engines)

                # Apply routing decision to candidate.
                candidate.add_query(
                    query_idx,
                    query,
                    routing_engine,
                    workload.get_predicted_analytical_latency(
                        query_idx, routing_engine
                    ),
                    ctx,
                )

                # Check if we need to scale up.
                candidate.try_to_make_feasible_if_needed(ctx)
                if candidate.feasibility == BlueprintFeasibility.Infeasible:
                    break

            candidate = provisioning_search(ctx, candidate)

            if candidate.feasibility != BlueprintFeasibility.Infeasible:
                if best_candidate is None or candidate.is_better_than(best_candidate):
                    best_candidate = candidate

        if best_candidate is None:
            # All sampled blueprints were infeasible.
            return None

        # Place tables that haven't been placed on any engine.
        place_unused_tables(ctx, best_candidate)

        # Print score of blueprint candidate.
        print("FINAL SCORE:")
        best_candidate.is_better_than(best_candidate, verbose=True)

        return (
            best_candidate.to_blueprint(ctx, use_legacy_behavior=False),
            best_candidate.to_score(),
        )

    def plan_greedy(
        self, workload: Workload, planning_router: Router, ctx: ScoringContext
    ) -> Optional[Tuple[Blueprint, Score]]:
        """
        Baseline: Place each query on the engine it is predicted to run
        best on, disregarding other queries and storage and provisioning
        costs. Choose table placement and provisioning accordingly.
        """
        latencies: npt.NDArray = (
            workload._predicted_analytical_latencies  # pylint: disable=protected-access
        )
        chosen_engine_idx = np.argmin(latencies, axis=1)

        # Initialize candidate and put all transactional tables on Aurora.
        blueprint = BlueprintCandidate.based_on(
            self._current_blueprint, self._comparator
        )
        blueprint.add_transactional_tables(ctx)

        # Greedily route queries to best engine.
        idx_to_eng = {v: k for k, v in workload.EngineLatencyIndex.items()}
        queries = workload.analytical_queries()
        assert chosen_engine_idx.shape[0] == len(queries)

        queries = queries[:self.n_queries]
        for query_idx, (query, eng_idx) in enumerate(zip(queries, chosen_engine_idx)):
            routing_engine = idx_to_eng[eng_idx]

            # Check if engine supports query's "specialized functionality".
            # If not: Select best one that does.
            supp_engines = Engine.from_bitmap(
                planning_router.run_functionality_routing(query)
            )

            if routing_engine not in supp_engines:
                supp_engines_score = [
                    (lat, idx)
                    for idx, lat in enumerate(latencies[query_idx, :])
                    if idx_to_eng[idx] in supp_engines
                ]
                _, routing_engine_idx = min(supp_engines_score)
                routing_engine = idx_to_eng[routing_engine_idx]

            # Apply routing decision to candidate.
            blueprint.add_query(
                query_idx,
                query,
                routing_engine,
                workload.get_predicted_analytical_latency(query_idx, routing_engine),
                ctx,
            )

            # Check if we need to scale up.
            blueprint.try_to_make_feasible_if_needed(ctx)
            if blueprint.feasibility == BlueprintFeasibility.Infeasible:
                # Assume that if not feasible now, we can't fix later.
                return None

        # Place tables that haven't been placed on any engine.
        place_unused_tables(ctx, blueprint)

        # Print score of blueprint candidate.
        print("FINAL SCORE:")
        blueprint.is_better_than(blueprint, verbose=True)

        return (
            blueprint.to_blueprint(ctx, use_legacy_behavior=False),
            blueprint.to_score(),
        )

    def plan_exhaustive(
        self, workload: Workload, planning_router: Router, ctx: ScoringContext
    ) -> Optional[Tuple[Blueprint, Score]]:
        """
        Baseline: Exhaustively search over all possible query placements.
        Choose table placement and provisioning accordingly.
        """
        best_candidate: Optional[BlueprintCandidate] = None
        engines = [Engine.Aurora, Engine.Redshift, Engine.Athena]
        queries = workload.analytical_queries()[:self.n_queries]
        routing_iter = product(engines, repeat=len(queries))
        cnt = 0
        total = 3**self.n_queries
        for routing in routing_iter:

            # print progress
            if cnt % 1000 == 0:
                print("Progress:", cnt/total)
            cnt += 1

            # Initialize candidate and put all transactional tables on Aurora.
            candidate = BlueprintCandidate.based_on(
                self._current_blueprint, self._comparator
            )
            candidate.add_transactional_tables(ctx)

            feasible = True
            for query_idx, (eng, query) in enumerate(zip(routing, queries)):
                # Check if chosen engine is lacking "specialized functionality".
                supp_engines = Engine.from_bitmap(
                    planning_router.run_functionality_routing(query)
                )
                if eng not in supp_engines:
                    feasible = False
                    break

                # Apply routing decision to candidate.
                candidate.add_query(
                    query_idx,
                    query,
                    eng,
                    workload.get_predicted_analytical_latency(query_idx, eng),
                    ctx,
                )

                # Check if we need to scale up
                candidate.try_to_make_feasible_if_needed(ctx)
                if candidate.feasibility == BlueprintFeasibility.Infeasible:
                    # Assume that if not feasible now, we can't fix later.
                    feasible = False
                    break

            # search provisionings
            candidate = provisioning_search(ctx, candidate)

            if feasible and (
                best_candidate is None or candidate.is_better_than(best_candidate)
            ):
                best_candidate = candidate

        if best_candidate is None:
            # Didn't find a feasible blueprint.
            return None

        # Place tables that haven't been placed on any engine.
        place_unused_tables(ctx, best_candidate)

        # Print best blueprint candidate
        best_blueprint = best_candidate.to_blueprint(ctx, use_legacy_behavior=False)
        logger.info("Selected blueprint:")
        logger.info("%s", best_blueprint)
        debug_values = best_candidate.to_debug_values()
        logger.info(
            "Selected blueprint details: %s", json.dumps(debug_values, indent=2)
        )

        # Print score of blueprint candidate.
        print("FINAL SCORE:")
        best_candidate.is_better_than(best_candidate, verbose=True)

        return (
            best_candidate.to_blueprint(ctx, use_legacy_behavior=False),
            best_candidate.to_score(),
        )

    async def _run_replan_impl(
        self, window_multiplier: int = 1
    ) -> Optional[Tuple[Blueprint, Score]]:
        logger.info(f"Running {self.baseline} baseline ...")

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

        self._comparator = self._providers.comparator_provider.get_comparator(
            metrics,
            curr_hourly_cost=0.44, # TODO: Hardcoded case as other
        )

        if self._planner_config.flag("ensure_tables_together_on_one_engine"):
            # This adds a constraint to ensure all tables are present together
            # on at least one engine. This ensures that arbitrary unseen join
            # templates can always be immediately handled.
            all_tables = ", ".join(
                [table.name for table in self._current_blueprint.tables()]
            )
            next_workload.add_priming_analytical_query(
                f"SELECT 1 FROM {all_tables} LIMIT 1"
            )

        # 2. Initialize planning state.
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
        # TODO: Below needed?
        await ctx.simulate_current_workload_routing(planning_router)
        ctx.compute_engine_latency_norm_factor()

        # 3. Run baseline
        baselines = {
            "random": self.plan_random,
            "greedy": self.plan_greedy,
            "exhaustive": self.plan_exhaustive,
        }
        try:
            baseline_fn = baselines[self.baseline]
        except KeyError:
            raise ValueError("Requested unkown baseline '{}'".format(self.baseline))

        return baseline_fn(next_workload, planning_router, ctx)