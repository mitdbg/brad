import importlib.resources as pkg_resources
import json
import logging
import math
from collections import namedtuple
from typing import Dict, Optional, Tuple

from .score import Scorer, Score, ScoringContext
import brad.planner.scoring.data as score_data

from brad.blueprint import Blueprint
from brad.blueprint.diff.blueprint import BlueprintDiff
from brad.blueprint.provisioning import Provisioning
from brad.config.engine import Engine
from brad.config.planner import PlannerConfig
from brad.routing.rule_based import RuleBased

logger = logging.getLogger(__name__)

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

ALL_METRICS = _REDSHIFT_METRICS + _AURORA_METRICS + _ATHENA_METRICS


class ScalingScorer(Scorer):
    def __init__(self, planner_config: PlannerConfig) -> None:
        self._planner_config = planner_config

    def score(self, ctx: ScoringContext) -> Score:
        self._simulate_next_workload(ctx)
        debug_components: Dict[str, int | float] = {}
        transition_score = self._transition_score(ctx, debug_components)
        op_cost_score = self._operational_cost_score(ctx, debug_components)
        perf_score = self._performance_score(ctx, debug_components)
        return Score(perf_score, op_cost_score, transition_score, debug_components)

    def _simulate_next_workload(self, ctx: ScoringContext) -> None:
        # NOTE: The routing policy should be included in the blueprint. We
        # currently hardcode it here for engineering convenience.
        router = RuleBased(blueprint=ctx.next_blueprint)

        # See where each analytical query gets routed.
        for q in ctx.next_workload.analytical_queries():
            next_engine = router.engine_for(q)
            ctx.next_dest[next_engine].append(q)
            q.populate_data_accessed_mb(next_engine, ctx.engines, ctx.current_blueprint)

    def _operational_cost_score(
        self, ctx: ScoringContext, debug_components: Dict[str, int | float]
    ) -> float:
        # Operational monetary score:
        # - Provisioning costs for an hour
        # - Aurora scans cost
        # - Athena scans cost

        # Provisioning costs.
        aurora_prov = ctx.next_blueprint.aurora_provisioning()
        redshift_prov = ctx.next_blueprint.redshift_provisioning()
        aurora_prov_cost = (
            _AURORA_SPECS[aurora_prov.instance_type()].usd_per_hour
            * aurora_prov.num_nodes()
        )
        redshift_prov_cost = (
            _REDSHIFT_SPECS[redshift_prov.instance_type()].usd_per_hour
            * redshift_prov.num_nodes()
        )

        aurora_access_mb = 0
        for q in ctx.next_dest[Engine.Aurora]:
            # Data accessed must always be populated using the current blueprint
            # (since the tables would not have been moved yet).
            q.populate_data_accessed_mb(
                for_engine=Engine.Aurora,
                connections=ctx.engines,
                blueprint=ctx.current_blueprint,
            )
            aurora_access_mb += q.data_accessed_mb(Engine.Aurora)

        athena_access_mb = 0
        for q in ctx.next_dest[Engine.Athena]:
            # Data accessed must always be populated using the current blueprint
            # (since the tables would not have been moved yet).
            q.populate_data_accessed_mb(
                for_engine=Engine.Athena,
                connections=ctx.engines,
                blueprint=ctx.current_blueprint,
            )
            athena_access_mb += q.data_accessed_mb(Engine.Athena)

        aurora_scan_cost = (
            aurora_access_mb * self._planner_config.aurora_usd_per_mb_scanned()
        )
        athena_scan_cost = (
            athena_access_mb * self._planner_config.athena_usd_per_mb_scanned()
        )

        debug_components["aurora_prov_cost"] = aurora_prov_cost
        debug_components["redshift_prov_cost"] = redshift_prov_cost
        debug_components["aurora_access_mb"] = aurora_access_mb
        debug_components["athena_access_mb"] = athena_access_mb
        debug_components["aurora_scan_cost"] = aurora_scan_cost
        debug_components["athena_scan_cost"] = athena_scan_cost

        operational_score = (
            aurora_prov_cost + redshift_prov_cost + aurora_scan_cost + athena_scan_cost
        )
        debug_components["operational_score"] = operational_score

        return operational_score

    def _transition_score(
        self, ctx: ScoringContext, debug_components: Dict[str, int | float]
    ) -> float:
        # Transition score:
        # - Table movement (size * transmission rate)
        # - Table movement monetary costs (Athena)
        # - Redshift scale up / down time
        # - Aurora scale up / down time
        if ctx.bp_diff is None:
            transition_score = 1.0

            debug_components["movement_time_s"] = 0.0
            debug_components["movement_cost"] = 0.0
            debug_components["aurora_prov_time_s"] = 0.0
            debug_components["redshift_prov_time_s"] = 0.0
        else:
            # Provisioning changes.
            redshift_prov_time_s = (
                self._planner_config.redshift_provisioning_change_time_s()
                if ctx.bp_diff.redshift_diff() is not None
                else 0
            )
            aurora_prov_time_s = (
                self._planner_config.aurora_provisioning_change_time_s()
                if ctx.bp_diff.aurora_diff() is not None
                else 0
            )

            # Table movement.
            movement_cost = 0.0
            movement_time_s = 0.0
            for tbl_diff in ctx.bp_diff.table_diffs():
                table_name = tbl_diff.table_name()
                move_to = tbl_diff.added_locations()
                if len(move_to) == 0:
                    # This means that we are only removing this table from
                    # engines. "Dropping" a table is "free".
                    continue

                move_from = self._best_extract_engine(ctx.current_blueprint, table_name)
                source_table_size_mb = ctx.current_workload.table_size_on_engine(
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
            if transition_time_s <= 0.0:
                transition_score = math.sqrt(transition_cost)
            elif transition_cost <= 0.0:
                transition_score = math.sqrt(transition_time_s)
            else:
                transition_score = math.sqrt(transition_time_s * transition_cost)

            debug_components["movement_time_s"] = movement_time_s
            debug_components["movement_cost"] = movement_cost
            debug_components["aurora_prov_time_s"] = aurora_prov_time_s
            debug_components["redshift_prov_time_s"] = redshift_prov_time_s

        debug_components["transition_score"] = transition_score
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
        self, ctx: ScoringContext, debug_components: Dict[str, int | float]
    ) -> Dict[str, float]:
        # > 1.0 means the dataset size has increased
        dataset_scaling = (
            ctx.next_workload.dataset_size_mb() / ctx.current_workload.dataset_size_mb()
        )
        # > 1.0 means there are more resources
        redshift_resource_scaling = self._compute_resource_scaling(
            ctx.current_blueprint, ctx.next_blueprint, ctx.bp_diff, Engine.Redshift
        )
        # > 1.0 means there are more resources
        aurora_resource_scaling = self._compute_resource_scaling(
            ctx.current_blueprint, ctx.next_blueprint, ctx.bp_diff, Engine.Aurora
        )

        inv_redshift_resource_scaling = 1.0 / redshift_resource_scaling
        inv_aurora_resource_scaling = 1.0 / aurora_resource_scaling

        dataset_modifiers = self._planner_config.dataset_scaling_modifiers()
        redshift_resource_modifiers = (
            self._planner_config.redshift_resource_scaling_modifiers()
        )
        aurora_resource_modifiers = (
            self._planner_config.aurora_resource_scaling_modifiers()
        )

        # Compute the table placement modifiers for each engine.
        def compute_table_modifier(engine):
            if ctx.current_total_accessed_mb[engine] == 0:
                modifier = 1.0
            else:
                accessed_mb = 0
                for q in ctx.next_dest[engine]:
                    accessed_mb += q.data_accessed_mb(engine)
                modifier = accessed_mb / ctx.current_total_accessed_mb[engine]
            return modifier

        aurora_tp_modifier = compute_table_modifier(Engine.Aurora)
        redshift_tp_modifier = compute_table_modifier(Engine.Redshift)
        athena_tp_modifier = compute_table_modifier(Engine.Athena)

        predicted_metrics: Dict[str, float] = {}

        # Redshift predictions.
        for metric_name in _REDSHIFT_METRICS:
            curr_value = ctx.metrics[metric_name].iloc[0]
            pred_value = curr_value
            pred_value *= dataset_scaling * dataset_modifiers[metric_name]
            pred_value *= (
                inv_redshift_resource_scaling * redshift_resource_modifiers[metric_name]
            )
            pred_value *= redshift_tp_modifier
            predicted_metrics[metric_name] = pred_value

        # Aurora predictions.
        # TODO: Model the difference between Aurora writer instances and read
        # replicas.
        # TODO: Model the interaction between transactions and analytics on
        # Aurora's metrics.
        for metric_name in _AURORA_METRICS:
            curr_value = ctx.metrics[metric_name].iloc[0]
            pred_value = curr_value
            pred_value *= dataset_scaling * dataset_modifiers[metric_name]
            pred_value *= (
                inv_aurora_resource_scaling * aurora_resource_modifiers[metric_name]
            )
            pred_value *= aurora_tp_modifier
            predicted_metrics[metric_name] = pred_value

        # Athena predictions.
        for metric_name in _ATHENA_METRICS:
            curr_value = ctx.metrics[metric_name].iloc[0]
            pred_value = curr_value
            pred_value *= dataset_scaling * dataset_modifiers[metric_name]
            pred_value *= athena_tp_modifier
            predicted_metrics[metric_name] = pred_value

        debug_components["aurora_tp_modifier"] = aurora_tp_modifier
        debug_components["athena_tp_modifier"] = athena_tp_modifier
        debug_components["redshift_tp_modifier"] = redshift_tp_modifier
        debug_components["dataset_scaling"] = dataset_scaling
        debug_components["redshift_resource_scaling"] = redshift_resource_scaling
        debug_components["aurora_resource_scaling"] = aurora_resource_scaling

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
            config["memory_mib"],
        )
        for config in raw_json
    }


_AURORA_SPECS = _load_instance_specs("aurora_postgresql_instances.json")
_REDSHIFT_SPECS = _load_instance_specs("redshift_instances.json")
