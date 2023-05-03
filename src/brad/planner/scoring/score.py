import math
from typing import Dict

from brad.blueprint import Blueprint
from brad.planner.workload import Workload
from brad.server.engine_connections import EngineConnections


class Score:
    def __init__(
        self,
        perf_metrics: Dict[str, float],
        monetary_cost_score: float,
        transition_score: float,
    ) -> None:
        # NOTE: For better performance debugging we should expand these
        # components out into more pieces.
        self._perf_metrics = perf_metrics
        self._monetary_cost_score = monetary_cost_score
        self._transition_score = transition_score

    def __repr__(self) -> str:
        score_components = "\n  ".join(
            [
                "single_value: {}".format(self.single_value()),
                "monetary_cost_score: {}".format(self._monetary_cost_score),
                "transition_score: {}".format(self._transition_score),
            ]
            + [f"{name}: {value}" for name, value in self._perf_metrics.items()]
        )
        return "Score:\n  " + score_components

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
