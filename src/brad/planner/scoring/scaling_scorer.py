import importlib.resources as pkg_resources
import json
import math
from typing import Dict, List

from .score import Scorer, Score
import brad.planner.scoring.data as score_data

from brad.blueprint import Blueprint
from brad.blueprint.diff.blueprint import BlueprintDiff
from brad.config.engine import Engine
from brad.config.planner import PlannerConfig
from brad.daemon.monitor import Monitor
from brad.planner.workload import Workload
from brad.planner.workload.query import Query
from brad.routing.rule_based import RuleBased
from brad.server.engine_connections import EngineConnections


class ScalingScorer(Scorer):
    def __init__(self, monitor: Monitor, planner_config: PlannerConfig) -> None:
        # For access to metrics.
        self._monitor = monitor
        self._planner_config = planner_config

    async def score(
        self,
        current_blueprint: Blueprint,
        next_blueprint: Blueprint,
        current_workload: Workload,
        next_workload: Workload,
        engines: EngineConnections,
    ) -> Score:
        return Score(
            1.0,
            await self._operational_cost_score(
                current_blueprint, next_blueprint, next_workload, engines
            ),
            self._transition_score(current_blueprint, next_blueprint, current_workload),
        )

    async def _operational_cost_score(
        self,
        current_blueprint: Blueprint,
        next_blueprint: Blueprint,
        next_workload: Workload,
        engines: EngineConnections,
    ) -> float:
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

        # NOTE: The routing policy should be included in the blueprint. We
        # currently hardcode it here for engineering convenience.
        router = RuleBased(blueprint=next_blueprint)

        dests: Dict[Engine, List[Query]] = {}
        dests[Engine.Aurora] = []
        dests[Engine.Athena] = []
        dests[Engine.Redshift] = []

        # See where each analytical query gets routed.
        for q in next_workload.analytical_queries():
            dests[router.engine_for(q)].append(q)

        aurora_access_mb = 0
        for q in dests[Engine.Aurora]:
            # Data accessed must always be populated using the current blueprint
            # (since the tables would not have been moved yet).
            await q.populate_data_accessed_mb(
                for_engine=Engine.Aurora,
                connections=engines,
                blueprint=current_blueprint,
            )
            aurora_access_mb += q.data_accessed_mb(Engine.Aurora)

        athena_access_mb = 0
        for q in dests[Engine.Athena]:
            # Data accessed must always be populated using the current blueprint
            # (since the tables would not have been moved yet).
            await q.populate_data_accessed_mb(
                for_engine=Engine.Athena,
                connections=engines,
                blueprint=current_blueprint,
            )
            athena_access_mb += q.data_accessed_mb(Engine.Athena)

        aurora_scan_cost = (
            aurora_access_mb * self._planner_config.aurora_usd_per_mb_scanned()
        )
        athena_scan_cost = (
            athena_access_mb * self._planner_config.athena_usd_per_mb_scanned()
        )

        return (
            aurora_prov_cost + redshift_prov_cost + aurora_scan_cost + athena_scan_cost
        )

    def _transition_score(
        self,
        current_blueprint: Blueprint,
        next_blueprint: Blueprint,
        current_workload: Workload,
    ) -> float:
        # Transition score:
        # - Table movement (size * transmission rate)
        # - Table movement monetary costs (Athena)
        # - Redshift scale up / down time
        # - Aurora scale up / down time
        bp_diff = BlueprintDiff.of(current_blueprint, next_blueprint)
        if bp_diff is None:
            transition_score = 1.0
        else:
            # Provisioning changes.
            redshift_prov_time_s = (
                self._planner_config.redshift_provisioning_change_time_s()
                if bp_diff.redshift_diff() is not None
                else 0
            )
            aurora_prov_time_s = (
                self._planner_config.aurora_provisioning_change_time_s()
                if bp_diff.aurora_diff() is not None
                else 0
            )

            # Table movement.
            movement_cost = 0.0
            movement_time_s = 0.0
            for tbl_diff in bp_diff.table_diffs():
                table_name = tbl_diff.table_name()
                move_to = tbl_diff.added_locations()
                move_from = self._best_extract_engine(current_blueprint, table_name)
                source_table_size_mb = current_workload.table_size_on_engine(
                    table_name, move_from
                )
                assert source_table_size_mb is not None

                # Extraction scoring.
                if move_from == Engine.Athena:
                    movement_time_s += (
                        source_table_size_mb
                        / self._planner_config.athena_extract_rate_mb_per_s()
                    )
                    movement_cost += (
                        self._planner_config.athena_usd_per_mb_scanned()
                        * source_table_size_mb
                    )

                elif move_from == Engine.Aurora:
                    movement_time_s += (
                        source_table_size_mb
                        / self._planner_config.aurora_extract_rate_mb_per_s()
                    )

                elif move_from == Engine.Redshift:
                    movement_time_s += (
                        source_table_size_mb
                        / self._planner_config.redshift_extract_rate_mb_per_s()
                    )

                # Import scoring.
                for into_loc in move_to:
                    # Need to assume the table will have the same size as on the
                    # source engine. This is not necessarily true when Redshift
                    # is the source, because it uses compression.
                    if into_loc == Engine.Athena:
                        movement_time_s += (
                            source_table_size_mb
                            / self._planner_config.athena_load_rate_mb_per_s()
                        )
                        movement_cost += (
                            self._planner_config.athena_usd_per_mb_scanned()
                            * source_table_size_mb
                        )

                    elif into_loc == Engine.Aurora:
                        movement_time_s += (
                            source_table_size_mb
                            / self._planner_config.aurora_load_rate_mb_per_s()
                        )

                    elif into_loc == Engine.Redshift:
                        movement_time_s += (
                            source_table_size_mb
                            / self._planner_config.redshift_load_rate_mb_per_s()
                        )

            transition_time_s = (
                redshift_prov_time_s + aurora_prov_time_s + movement_time_s
            )
            transition_cost = movement_cost
            transition_score = math.sqrt(
                (1.0 + transition_time_s) * (1.0 + transition_cost)
            )

        return transition_score

    def _best_extract_engine(self, blueprint: Blueprint, table_name: str) -> Engine:
        """
        Returns the best source engine to extract a table from.
        """
        options = []
        for loc in blueprint.get_table_locations(table_name):
            if loc == Engine.Aurora:
                options.append(
                    (loc, self._planner_config.aurora_extract_rate_mb_per_s())
                )
            elif loc == Engine.Athena:
                options.append(
                    (loc, self._planner_config.athena_extract_rate_mb_per_s())
                )
            elif loc == Engine.Redshift:
                options.append(
                    (loc, self._planner_config.redshift_extract_rate_mb_per_s())
                )
        options.sort(key=lambda op: op[1])
        if len(options) > 1 and options[0][0] == Engine.Athena:
            # Avoid Athena if possible because we need to pay for extraction.
            return options[1][0]
        else:
            return options[0][0]


def _load_instance_pricing(file_name: str) -> Dict[str, float]:
    with pkg_resources.open_text(score_data, file_name) as data:
        raw_json = json.load(data)

    return {config["instance_type"]: config["usd_per_hour"] for config in raw_json}


_AURORA_PRICING = _load_instance_pricing("aurora_postgresql_instances.json")
_REDSHIFT_PRICING = _load_instance_pricing("redshift_instances.json")
