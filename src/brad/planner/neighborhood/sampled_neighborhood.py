import logging
import os
from typing import List

from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.monitor import Monitor
from brad.planner import BlueprintPlanner
from brad.planner.logger import BlueprintPlanningLogger
from brad.planner.blueprint_candidate import BlueprintCandidate
from brad.planner.enumeration.blueprint import EnumeratedBlueprint
from brad.planner.neighborhood.neighborhood import (
    NeighborhoodSearchPlanner,
    NeighborhoodImpl,
)
from brad.planner.scoring.scaling_scorer import ScalingScorer
from brad.planner.scoring.score import ScoringContext
from brad.planner.workload import Workload
from brad.utils.reservoir_sampler import ReservoirSampler

logger = logging.getLogger(__name__)

LOG_REPLAN_VAR = "BRAD_LOG_PLANNING"


class SampledNeighborhoodSearchPlanner(BlueprintPlanner, NeighborhoodImpl):
    def __init__(
        self,
        current_blueprint: Blueprint,
        current_workload: Workload,
        planner_config: PlannerConfig,
        monitor: Monitor,
        config: ConfigFile,
        schema_name: str,
    ) -> None:
        super().__init__()
        self._scorer = ScalingScorer(planner_config)
        self._planner = NeighborhoodSearchPlanner(
            current_blueprint,
            current_workload,
            planner_config,
            monitor,
            config,
            schema_name,
            self,
        )

        self._sampler = ReservoirSampler[Blueprint](planner_config.sample_set_size())

    async def run_forever(self) -> None:
        await self._planner.run_forever()

    async def run_replan(self) -> None:
        await self._planner.run_replan()

    def on_start_enumeration(self) -> None:
        self._sampler.reset()

    def on_enumerated_blueprint(
        self, bp: EnumeratedBlueprint, ctx: ScoringContext
    ) -> None:
        # Very important to call `.to_blueprint()` to make a copy of the
        # blueprint. The enumerator modifies the blueprint in place to avoid
        # creating many short-lived objects.
        # pylint: disable-next=unnecessary-lambda
        self._sampler.offer(lambda: bp.to_blueprint())

    async def on_enumeration_complete(self, ctx: ScoringContext) -> Blueprint:
        candidates: List[BlueprintCandidate] = []
        for bp in self._sampler.get():
            ctx.reset(bp)
            score = self._scorer.score(ctx)
            candidates.append(BlueprintCandidate(bp, score))

        if LOG_REPLAN_VAR in os.environ:
            bp_logger = BlueprintPlanningLogger()
            for bpc in candidates:
                bp_logger.log_blueprint_and_score(bpc.blueprint, bpc.score)

        # Sort by score - lower is better.
        candidates.sort(key=lambda bpc: bpc.score_value)

        # Log the top 10 candidate plans.
        for candidate in candidates[:10]:
            logger.debug("%s", candidate.score)
            logger.debug("%s", candidate.blueprint)
            logger.debug("----------")

        if len(candidates) == 0:
            logger.error(
                "Sampling planner did not find any valid candidate blueprints."
            )
            raise RuntimeError("No valid candidates!")

        best_blueprint = candidates[0].blueprint
        best_score = candidates[0].score
        logger.info("Selecting a new blueprint with score %s", best_score)
        logger.info("%s", best_blueprint)

        await self._notify_new_blueprint(best_blueprint)
        return best_blueprint
