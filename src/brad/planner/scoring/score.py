import math
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
        monetary_cost: float,
        transition_time_s: float,
        debug_components: Dict[str, int | float],
    ) -> None:
        self._perf_metrics = perf_metrics
        self._monetary_cost = monetary_cost
        self._transition_time_s = transition_time_s
        self._debug_components = debug_components
        self._debug_components["perf_summary_value"] = self.perf_summary_value()
        self._debug_components["monetary_cost"] = self.monetary_cost()
        self._debug_components["single_value"] = self.single_value()

    def __repr__(self) -> str:
        score_components = "\n  ".join(
            [f"{name}: {value}" for name, value in self._debug_components.items()]
            + [f"{name}: {value}" for name, value in self._perf_metrics.items()]
        )
        return "Score:\n  " + score_components

    def single_value(self) -> float:
        # To stay consistent with the other score components, lower is better.
        # N.B. This is a placeholder.
        num_components = 3
        zero = 1e-9
        values = []

        perf_value = self.perf_summary_value()
        if perf_value > zero:
            values.append(perf_value)
        if self._monetary_cost > zero:
            values.append(self._monetary_cost)
        if self._transition_time_s > zero:
            # We want to decrease the weight of the transition time since we
            # assume transitions occur during maintenance windows and that they
            # can complete during the maintenance window. But we still want to
            # capture the fact that there is a time cost to a transition (some
            # blueprints are more expensive to transition to).
            # N.B. This weight was arbitrarily chosen.
            values.append(math.pow(self._transition_time_s, 0.25))

        npvalues = np.array(values)
        gmean = np.exp(np.log(npvalues).sum() / num_components)
        return gmean.item()

    def perf_summary_value(self) -> float:
        # To stay consistent with the cost and transition time component, lower is better.
        # We invert throughput values.
        num_components = 0
        zero = 1e-9
        values = []
        for metric, mvalue in self._perf_metrics.items():
            num_components += 1
            if mvalue <= zero:
                continue
            if "IOPS" in metric:
                values.append(1.0 / mvalue)
            else:
                values.append(mvalue)

        npvalues = np.array(values)
        gmean = np.exp(np.log(npvalues).sum() / num_components)
        return gmean.item()

    def monetary_cost(self) -> float:
        return self._monetary_cost

    def transition_time_s(self) -> float:
        return self._transition_time_s

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
