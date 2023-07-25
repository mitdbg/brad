import heapq
import logging
import os
from typing import List, Optional

from brad.blueprint import Blueprint
from brad.config.planner import PlannerConfig
from brad.planner.enumeration.blueprint import EnumeratedBlueprint
from brad.planner.neighborhood.logger import BlueprintPlanningLogger
from brad.planner.neighborhood.blueprint_candidate import BlueprintCandidate
from brad.planner.neighborhood.impl import NeighborhoodImpl
from brad.planner.neighborhood.scaling_scorer import ScalingScorer
from brad.planner.neighborhood.score import ScoringContext

logger = logging.getLogger(__name__)

LOG_REPLAN_VAR = "BRAD_LOG_PLANNING"


class FullNeighborhoodSearchPlanner(NeighborhoodImpl):
    def __init__(self, planner_config: PlannerConfig) -> None:
        super().__init__()
        self._scorer = ScalingScorer(planner_config)

        # No need to keep around all candidates if we are selecting the best
        # blueprint. But for debugging purposes it is useful to see what
        # blueprints are being considered.
        self._candidate_set: List[BlueprintCandidate] = []
        self._num_top = 50

        self._bp_logger: Optional[BlueprintPlanningLogger] = None

    def on_start_enumeration(self) -> None:
        self._candidate_set.clear()
        if LOG_REPLAN_VAR in os.environ:
            self._bp_logger = BlueprintPlanningLogger()

    def on_enumerated_blueprint(
        self, bp: EnumeratedBlueprint, ctx: ScoringContext
    ) -> None:
        # Score the blueprint.
        ctx.reset(bp)
        score = self._scorer.score(ctx)
        if self._bp_logger is not None:
            self._bp_logger.log_blueprint_and_score(bp, score)

        # Store the blueprint (for debugging purposes).
        if len(self._candidate_set) < self._num_top:
            self._candidate_set.append(BlueprintCandidate(bp.to_blueprint(), score))
            if len(self._candidate_set) == self._num_top:
                heapq.heapify(self._candidate_set)
        elif self._candidate_set[0].score_value > score.single_value():
            # Replace the "worst" blueprint so far with this one (lower
            # score is better).
            latest = BlueprintCandidate(bp.to_blueprint(), score)
            heapq.heappushpop(self._candidate_set, latest)

    def on_enumeration_complete(self, _ctx: ScoringContext) -> Blueprint:
        # Close the logger.
        self._bp_logger = None

        # Sort by score - lower is better.
        self._candidate_set.sort(key=lambda bpc: bpc.score_value)

        # Log the top 10 candidate plans.
        for candidate in self._candidate_set[:10]:
            logger.debug("%s", candidate.score)
            logger.debug("%s", candidate.blueprint)
            logger.debug("----------")

        if len(self._candidate_set) == 0:
            logger.error("Planner did not find any valid candidate blueprints.")
            raise RuntimeError("No valid candidates!")

        best_blueprint = self._candidate_set[0].blueprint
        best_score = self._candidate_set[0].score
        logger.info("Selecting a new blueprint with score %s", best_score)
        logger.info("%s", best_blueprint)

        return best_blueprint
