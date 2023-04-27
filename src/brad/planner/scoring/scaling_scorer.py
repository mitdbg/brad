import importlib.resources as pkg_resources
import json
from typing import Dict

from .score import Scorer, Score
import brad.planner.scoring.data as score_data
from brad.blueprint import Blueprint
from brad.blueprint.diff.blueprint import BlueprintDiff
from brad.daemon.monitor import Monitor
from brad.planner.workload import Workload


class ScalingScorer(Scorer):
    def __init__(self, monitor: Monitor) -> None:
        # For access to metrics.
        self._monitor = monitor

    def score(
        self,
        current_blueprint: Blueprint,
        next_blueprint: Blueprint,
        current_workload: Workload,
        next_workload: Workload,
    ) -> Score:
        # Operational monetary score:
        # - Provisioning costs for an hour
        # - Aurora scans cost
        # - Athena scans cost

        # Provisioning costs.
        aurora_prov = next_blueprint.aurora_provisioning()
        redshift_prov = next_blueprint.redshift_provisioning()
        aurora_prov_cost = (
            _AURORA_PRICING[aurora_prov.instance_type()] * aurora_prov.num_nodes()
        )
        redshift_prov_cost = (
            _REDSHIFT_PRICING[redshift_prov.instance_type()] * redshift_prov.num_nodes()
        )
        _total_prov_cost = aurora_prov_cost + redshift_prov_cost

        # TODO: Scan costs.

        # Transition score:
        # - Table movement (size * transmission rate)
        # - Table movement monetary costs (Athena)
        # - Redshift scale up / down time
        # - Aurora scale up / down time
        _bp_diff = BlueprintDiff.of(current_blueprint, next_blueprint)

        # N.B. This is a placeholder value.
        return Score(1.0, 1.0, 1.0)


def _load_instance_pricing(file_name: str) -> Dict[str, float]:
    with pkg_resources.open_text(score_data, file_name) as data:
        raw_json = json.load(data)

    return {config["instance_type"]: config["usd_per_hour"] for config in raw_json}


_AURORA_PRICING = _load_instance_pricing("aurora_postgresql_instances.json")
_REDSHIFT_PRICING = _load_instance_pricing("redshift_instances.json")
