import logging
import random
from datetime import timedelta
from typing import Tuple, Optional

import npt as npt
import numpy as np
import numpy.typing as npt
from itertools import product

from brad.blueprint.blueprint import Blueprint
from brad.config.engine import Engine
from brad.planner.abstract import BlueprintPlanner
from brad.planner.beam.feasibility import BlueprintFeasibility
from brad.planner.beam.query_based_candidate import BlueprintCandidate
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.score import Score
from brad.planner.workload import Workload
from brad.routing.router import Router


logger = logging.getLogger(__name__)


class BaselinePlanner(BlueprintPlanner):
    def __init__(
        self,
        *args,
        baseline,
        num_samples=10000,
        random_seed=42,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.baseline = baseline
        self.num_samples = num_samples
        self.prng = random.Random(random_seed)

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

            for query_idx, query in enumerate(workload.analytical_queries()):
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

            if candidate.feasibility != BlueprintFeasibility.Infeasible:
                if best_candidate is None or candidate.is_better_than(best_candidate):
                    best_candidate = candidate

        if best_candidate is None:
            # All sampled blueprints were infeasible.
            return None

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
        queries = workload.analytical_queries()
        routing_iter = product(engines, repeat=len(queries))
        for routing in routing_iter:
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

            if feasible and (
                best_candidate is None or candidate.is_better_than(best_candidate)
            ):
                best_candidate = candidate

        if best_candidate is None:
            # Didn't find a feasible blueprint.
            return None

        return (
            best_candidate.to_blueprint(ctx, use_legacy_behavior=False),
            best_candidate.to_score(),
        )

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
