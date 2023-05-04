import numpy as np
import pandas as pd
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
        perf_debugging: Dict[str, float],
    ) -> None:
        # NOTE: For better performance debugging we should expand these
        # components out into more pieces.
        self._perf_metrics = perf_metrics
        self._monetary_cost_score = monetary_cost_score
        self._transition_score = transition_score
        self._perf_debugging = perf_debugging

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
        # To stay consistent with the other score components, lower is better.
        # We invert throughput values.
        # N.B. This is a placeholder.
        values = [self._monetary_cost_score, self._transition_score]
        for metric, mvalue in self._perf_metrics.items():
            if mvalue <= 0.0:
                continue
            if "IOPS" in metric:
                values.append(1.0 / mvalue)
            else:
                values.append(mvalue)

        npvalues = np.array(values)
        gmean = np.exp(np.log(npvalues).mean())
        return gmean.item()

    def perf_debugging(self) -> Dict[str, float]:
        return self._perf_debugging


class Scorer:
    def score(
        self,
        current_blueprint: Blueprint,
        next_blueprint: Blueprint,
        current_workload: Workload,
        next_workload: Workload,
        engines: EngineConnections,
        metrics: pd.DataFrame,
    ) -> Score:
        raise NotImplementedError
