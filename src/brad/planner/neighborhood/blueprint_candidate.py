from brad.blueprint import Blueprint
from brad.planner.neighborhood.score import Score


class BlueprintCandidate:
    def __init__(self, blueprint: Blueprint, score: Score) -> None:
        self.blueprint = blueprint
        self.score = score
        self.score_value = score.single_value()

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, BlueprintCandidate):
            return False
        # N.B. We invert this __lt__ definition since we want to use it with
        # `heapq` to create a max-heap (highest score at index 0).
        return self.score_value > other.score_value
