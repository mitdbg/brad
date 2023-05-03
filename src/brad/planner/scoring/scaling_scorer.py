import importlib.resources as pkg_resources
import json
import math
from collections import namedtuple
from typing import Dict, List, Optional, Tuple

from .score import Scorer, Score
import brad.planner.scoring.data as score_data

from brad.blueprint import Blueprint
from brad.blueprint.diff.blueprint import BlueprintDiff
from brad.blueprint.provisioning import Provisioning
from brad.config.engine import Engine
from brad.config.planner import PlannerConfig
from brad.daemon.monitor import Monitor
from brad.planner.workload import Workload
from brad.planner.workload.query import Query
from brad.routing.rule_based import RuleBased
from brad.server.engine_connections import EngineConnections

_REDSHIFT_METRICS = [
    "redshift_CPUUtilization_Average",
    "redshift_ReadIOPS_Average",
]

_AURORA_METRICS = [
    "aurora_WRITER_CPUUtilization_Average",
    "aurora_WRITER_ReadIOPS_Average",
    "aurora_WRITER_WriteIOPS_Average",
]

_ATHENA_METRICS = [
    "athena_TotalExecutionTime_Sum",
]

_ALL_METRICS = _REDSHIFT_METRICS + _AURORA_METRICS + _ATHENA_METRICS


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
        bp_diff = BlueprintDiff.of(current_blueprint, next_blueprint)
        return Score(
            {},
            await self._operational_cost_score(
                current_blueprint, next_blueprint, next_workload, engines
            ),
            self._transition_score(current_blueprint, bp_diff, current_workload),
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
            _AURORA_SPECS[aurora_prov.instance_type()].usd_per_hour
            * aurora_prov.num_nodes()
        )
        redshift_prov_cost = (
            _REDSHIFT_SPECS[redshift_prov.instance_type()].usd_per_hour
            * redshift_prov.num_nodes()
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
        bp_diff: Optional[BlueprintDiff],
        current_workload: Workload,
    ) -> float:
        # Transition score:
        # - Table movement (size * transmission rate)
        # - Table movement monetary costs (Athena)
        # - Redshift scale up / down time
        # - Aurora scale up / down time
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

    def _performance_score(
        self,
        current_blueprint: Blueprint,
        next_blueprint: Blueprint,
        bp_diff: Optional[BlueprintDiff],
        current_workload: Workload,
        next_workload: Workload,
    ) -> Dict[str, float]:
        # > 1.0 means the dataset size has increased
        dataset_scaling = (
            next_workload.dataset_size_mb() / current_workload.dataset_size_mb()
        )
        # > 1.0 means there are more resources
        redshift_resource_scaling = self._compute_resource_scaling(
            current_blueprint, next_blueprint, bp_diff, Engine.Redshift
        )
        # > 1.0 means there are more resources
        aurora_resource_scaling = self._compute_resource_scaling(
            current_blueprint, next_blueprint, bp_diff, Engine.Aurora
        )

        inv_redshift_resource_scaling = 1.0 / redshift_resource_scaling
        inv_aurora_resource_scaling = 1.0 / aurora_resource_scaling

        metrics_df = self._monitor.read_k_most_recent(metric_ids=_ALL_METRICS)

        dataset_modifiers = self._planner_config.dataset_scaling_modifiers()
        redshift_resource_modifiers = (
            self._planner_config.redshift_resource_scaling_modifiers()
        )
        aurora_resource_modifiers = (
            self._planner_config.aurora_resource_scaling_modifiers()
        )

        # TODO: Apply modifiers based on a changed table placement.

        predicted_metrics: Dict[str, float] = {}

        # Redshift predictions.
        for metric_name in _REDSHIFT_METRICS:
            curr_value = metrics_df[metric_name].iloc[0]
            pred_value = curr_value
            pred_value *= dataset_scaling * dataset_modifiers[metric_name]
            pred_value *= (
                inv_redshift_resource_scaling * redshift_resource_modifiers[metric_name]
            )
            predicted_metrics[metric_name] = pred_value

        # Aurora predictions.
        # TODO: Model the difference between Aurora writer instances and read
        # replicas.
        # TODO: Model the interaction between transactions and analytics on
        # Aurora's metrics.
        for metric_name in _AURORA_METRICS:
            curr_value = metrics_df[metric_name].iloc[0]
            pred_value = curr_value
            pred_value *= dataset_scaling * dataset_modifiers[metric_name]
            pred_value *= (
                inv_aurora_resource_scaling * aurora_resource_modifiers[metric_name]
            )
            predicted_metrics[metric_name] = pred_value

        # Athena predictions.
        for metric_name in _ATHENA_METRICS:
            curr_value = metrics_df[metric_name].iloc[0]
            pred_value = curr_value
            pred_value *= dataset_scaling * dataset_modifiers[metric_name]
            predicted_metrics[metric_name] = pred_value

        return predicted_metrics

    def _compute_resource_scaling(
        self,
        current_blueprint: Blueprint,
        next_blueprint: Blueprint,
        bp_diff: Optional[BlueprintDiff],
        engine: Engine,
    ) -> float:
        if bp_diff is None:
            # No provisioning change.
            return 1.0

        if engine == Engine.Redshift:
            diff = bp_diff.redshift_diff()
            if diff is None:
                # No Redshift provisioning change.
                return 1.0
            if (
                current_blueprint.redshift_provisioning().num_nodes() == 0
                or next_blueprint.redshift_provisioning().num_nodes() == 0
            ):
                # This engine is/will be disabled.
                return 0.0
            curr_specs = self._retrieve_provisioning_specs(
                engine, current_blueprint.redshift_provisioning()
            )
            next_specs = self._retrieve_provisioning_specs(
                engine, next_blueprint.redshift_provisioning()
            )

        elif engine == Engine.Aurora:
            diff = bp_diff.aurora_diff()
            if diff is None:
                # No Aurora provisioning change.
                return 1.0
            if (
                current_blueprint.aurora_provisioning().num_nodes() == 0
                or next_blueprint.aurora_provisioning().num_nodes() == 0
            ):
                # This engine is/will be disabled.
                return 0.0
            curr_specs = self._retrieve_provisioning_specs(
                engine, current_blueprint.aurora_provisioning()
            )
            next_specs = self._retrieve_provisioning_specs(
                engine, next_blueprint.aurora_provisioning()
            )

        else:
            raise RuntimeError("Unsupported resource scaling engine {}".format(engine))

        cpu_scale = next_specs[0] / curr_specs[0]
        mem_scale = next_specs[1] / curr_specs[1]

        return math.sqrt(cpu_scale * mem_scale)

    def _retrieve_provisioning_specs(
        self, engine: Engine, provisioning: Provisioning
    ) -> Tuple[int, int]:
        if engine == Engine.Redshift:
            specs = _REDSHIFT_SPECS[provisioning.instance_type()]
        elif engine == Engine.Aurora:
            specs = _AURORA_SPECS[provisioning.instance_type()]
        else:
            raise RuntimeError("Unsupported resource scaling engine {}".format(engine))
        return (
            specs.vcpus * provisioning.num_nodes(),
            specs.mem_mib * provisioning.num_nodes(),
        )


_Provisioning = namedtuple(
    "_Provisioning", ["instance_type", "usd_per_hour", "vcpus", "mem_mib"]
)


def _load_instance_specs(file_name: str) -> Dict[str, _Provisioning]:
    with pkg_resources.open_text(score_data, file_name) as data:
        raw_json = json.load(data)

    return {
        config["instance_type"]: _Provisioning(
            config["instance_type"],
            config["usd_per_hour"],
            config["vcpus"],
            config["mem_mib"],
        )
        for config in raw_json
    }


_AURORA_SPECS = _load_instance_specs("aurora_postgresql_instances.json")
_REDSHIFT_SPECS = _load_instance_specs("redshift_instances.json")
