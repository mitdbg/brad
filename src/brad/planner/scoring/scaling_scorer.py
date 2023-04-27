from .score import Scorer, Score
from brad.blueprint import Blueprint
from brad.planner.workload import Workload


class ScalingScorer(Scorer):
    def score(
        self,
        current_blueprint: Blueprint,
        next_blueprint: Blueprint,
        current_workload: Workload,
        next_workload: Workload,
    ) -> Score:
        # N.B. This is a placeholder value.
        return Score(1.0, 1.0, 1.0)
