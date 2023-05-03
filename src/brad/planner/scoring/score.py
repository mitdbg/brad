import math

from brad.blueprint import Blueprint
from brad.planner.workload import Workload
from brad.server.engine_connections import EngineConnections


class Score:
    def __init__(
        self,
        perf_score: float,
        monetary_cost_score: float,
        transition_score: float,
    ) -> None:
        # NOTE: For better performance debugging we should expand these
        # components out into more pieces.
        self._perf_score = perf_score
        self._monetary_cost_score = monetary_cost_score
        self._transition_score = transition_score

    def __repr__(self) -> str:
        return "".join(
            [
                "Score(perf_score=",
                str(self._perf_score),
                ", monetary_cost_score=",
                str(self._monetary_cost_score),
                ", transition_score=",
                str(self._transition_score),
                ")",
            ]
        )

    def single_value(self) -> float:
        # N.B. This is a placeholder.
        return math.pow(
            self._perf_score * self._monetary_cost_score * self._transition_score, 1 / 3
        )


class Scorer:
    async def score(
        self,
        current_blueprint: Blueprint,
        next_blueprint: Blueprint,
        current_workload: Workload,
        next_workload: Workload,
        engines: EngineConnections,
    ) -> Score:
        raise NotImplementedError
