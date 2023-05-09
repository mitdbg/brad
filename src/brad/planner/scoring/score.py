import numpy as np
import pandas as pd
from typing import Dict, List, Optional

from brad.blueprint import Blueprint
from brad.blueprint.diff.blueprint import BlueprintDiff
from brad.config.engine import Engine
from brad.planner.workload import Workload
from brad.planner.workload.query import Query
from brad.server.engine_connections import EngineConnections


class Score:
    def __init__(
        self,
        perf_metrics: Dict[str, float],
        monetary_cost_score: float,
        transition_score: float,
        debug_components: Dict[str, int | float],
    ) -> None:
        # NOTE: For better performance debugging we should expand these
        # components out into more pieces.
        self._perf_metrics = perf_metrics
        self._monetary_cost_score = monetary_cost_score
        self._transition_score = transition_score
        self._debug_components = debug_components

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

    def perf_metrics(self) -> Dict[str, float]:
        return self._perf_metrics

    def debug_components(self) -> Dict[str, int | float]:
        return self._debug_components


class ScoringContext:
    def __init__(
        self,
        current_blueprint: Blueprint,
        current_workload: Workload,
        next_workload: Workload,
        engines: EngineConnections,
        metrics: pd.DataFrame,
        current_total_accessed_mb: Dict[Engine, int],
    ) -> None:
        self.current_blueprint = current_blueprint
        self.current_workload = current_workload
        self.next_workload = next_workload
        self.engines = engines
        self.metrics = metrics

        self._next_blueprint: Optional[Blueprint] = None
        self.bp_diff: Optional[BlueprintDiff] = None

        # The total amount of data accessed (estimated) on each engine for the
        # current workload and current blueprint.
        self.current_total_accessed_mb = current_total_accessed_mb

        # Queries from the next workload that will be routed to each engine
        # under the next blueprint.
        self.next_dest: Dict[Engine, List[Query]] = {}
        self.next_dest[Engine.Aurora] = []
        self.next_dest[Engine.Athena] = []
        self.next_dest[Engine.Redshift] = []

    @property
    def next_blueprint(self) -> Blueprint:
        assert self._next_blueprint is not None
        return self._next_blueprint

    def reset(self, next_blueprint: Blueprint) -> None:
        self._next_blueprint = next_blueprint
        self.bp_diff = BlueprintDiff.of(self.current_blueprint, self._next_blueprint)
        self.next_dest[Engine.Aurora].clear()
        self.next_dest[Engine.Athena].clear()
        self.next_dest[Engine.Redshift].clear()


class Scorer:
    def score(self, ctx: ScoringContext) -> Score:
        raise NotImplementedError
