import math
import numpy as np
import numpy.typing as npt
from typing import Any, Dict, List, Optional

from brad.blueprint import Blueprint
from brad.blueprint.provisioning import Provisioning, MutableProvisioning
from brad.config.engine import Engine, EngineBitmapValues
from brad.planner.beam.feasibility import BlueprintFeasibility
from brad.planner.compare.blueprint import ComparableBlueprint
from brad.planner.compare.function import BlueprintComparator
from brad.planner.enumeration.provisioning import ProvisioningEnumerator
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.performance.load_factor import (
    scale_redshift_run_time_by_load,
)
from brad.planner.scoring.performance.cpu import (
    compute_next_redshift_cpu,
)
from brad.planner.scoring.performance.provisioning_scaling import (
    scale_redshift_predicted_latency,
)
from brad.planner.scoring.performance.unified_aurora import AuroraProvisioningScore
from brad.planner.scoring.provisioning import (
    compute_aurora_hourly_operational_cost,
    compute_redshift_hourly_operational_cost,
    compute_aurora_scan_cost,
    compute_aurora_accessed_pages,
    compute_athena_scan_cost,
    compute_athena_scanned_bytes,
    compute_aurora_transition_time_s,
    compute_redshift_transition_time_s,
)
from brad.planner.scoring.table_placement import (
    compute_single_athena_table_cost,
    compute_single_table_movement_time_and_cost,
)
from brad.planner.workload.query import Query


class BlueprintCandidate(ComparableBlueprint):
    """
    A "barebones" representation of a blueprint, used during the query-based
    optimization process.
    """

    @classmethod
    def based_on(
        cls, blueprint: Blueprint, comparator: BlueprintComparator
    ) -> "BlueprintCandidate":
        return cls(
            blueprint,
            blueprint.aurora_provisioning().mutable_clone(),
            blueprint.redshift_provisioning().mutable_clone(),
            {t.name: 0 for t in blueprint.tables()},
            comparator,
        )

    def __init__(
        self,
        source: Blueprint,
        aurora: MutableProvisioning,
        redshift: MutableProvisioning,
        table_placements: Dict[str, int],
        comparator: BlueprintComparator,
    ) -> None:
        self.aurora_provisioning = aurora.mutable_clone()
        self.redshift_provisioning = redshift.mutable_clone()
        # Table locations are represented using a bitmap. We initialize each
        # table to being present on no engines.
        self.table_placements = table_placements

        self._source_blueprint = source
        self._comparator = comparator

        self.query_locations: Dict[Engine, List[int]] = {}
        self.query_locations[Engine.Aurora] = []
        self.query_locations[Engine.Redshift] = []
        self.query_locations[Engine.Athena] = []

        self.base_query_latencies: Dict[Engine, List[float]] = {}
        self.base_query_latencies[Engine.Aurora] = []
        self.base_query_latencies[Engine.Redshift] = []
        self.base_query_latencies[Engine.Athena] = []

        # Scoring components.

        # Monetary costs.
        self.provisioning_cost = 0.0
        self.storage_cost = 0.0
        self.table_movement_trans_cost = 0.0

        # We compute the total scan cost from the Athena and Aurora data access
        # statistics. We do not compute the workload scan cost itself
        # incrementally to avoid numerical precision errors over large
        # workloads.
        self.workload_scan_cost = 0.0
        self.athena_scanned_bytes = 0
        self.aurora_accessed_pages = 0

        # Transition times.
        self.table_movement_trans_time_s = 0.0
        self.provisioning_trans_time_s = 0.0

        # Used for scoring purposes.
        self.explored_provisionings = False
        self.feasibility = BlueprintFeasibility.Unchecked
        self.scaled_query_latencies: Dict[Engine, npt.NDArray] = {}
        self.aurora_score: Optional[AuroraProvisioningScore] = None
        self.redshift_cpu = np.nan

        # Used during comparisons.
        self._memoized: Dict[str, Any] = {}

    def to_blueprint(self) -> Blueprint:
        # We use the source blueprint for table schema information.
        return Blueprint(
            self._source_blueprint.schema_name(),
            self._source_blueprint.tables(),
            self.get_table_placement(),
            self.aurora_provisioning.clone(),
            self.redshift_provisioning.clone(),
            self._source_blueprint.router_provider(),
        )

    def to_debug_values(self) -> Dict[str, int | float | str]:
        values: Dict[str, int | float | str] = self._memoized.copy()

        # Provisioning.
        values["aurora_instance"] = self.aurora_provisioning.instance_type()
        values["aurora_nodes"] = self.aurora_provisioning.num_nodes()
        values["redshift_instance"] = self.redshift_provisioning.instance_type()
        values["redshift_nodes"] = self.redshift_provisioning.num_nodes()

        # Tables placements.
        for tbl, bitmap in self.table_placements.items():
            values["table_{}".format(tbl)] = bitmap

        # Query breakdowns (rough).
        values["aurora_queries"] = len(self.query_locations[Engine.Aurora])
        values["redshift_queries"] = len(self.query_locations[Engine.Redshift])
        values["athena_queries"] = len(self.query_locations[Engine.Athena])

        # Scoring components.
        values["provisioning_cost"] = self.provisioning_cost
        values["storage_cost"] = self.storage_cost
        values["workload_scan_cost"] = self.workload_scan_cost
        values["athena_scanned_bytes"] = self.athena_scanned_bytes
        values["aurora_accessed_pages"] = self.aurora_accessed_pages
        values["table_movement_trans_cost"] = self.table_movement_trans_cost
        values["table_movement_trans_time_s"] = self.table_movement_trans_time_s
        values["provisioning_trans_time_s"] = self.provisioning_trans_time_s

        values["redshift_cpu"] = self.redshift_cpu

        if self.aurora_score is not None:
            values["aurora_load"] = self.aurora_score.overall_system_load
            values["aurora_cpu_denorm"] = self.aurora_score.overall_cpu_denorm
            values[
                "pred_txn_peak_cpu_denorm"
            ] = self.aurora_score.pred_txn_peak_cpu_denorm
            values.update(self.aurora_score.debug_values)

        return values

    def add_query(
        self,
        query_idx: int,
        query: Query,
        location: Engine,
        base_latency: float,
        ctx: ScoringContext,
    ) -> None:
        self.query_locations[location].append(query_idx)
        self.base_query_latencies[location].append(base_latency)
        engine_bitvalue = EngineBitmapValues[location]

        # Ensure that the table is present on the engine on which we want to run
        # the query.
        table_diffs = []
        for table_name in query.tables():
            try:
                orig = self.table_placements[table_name]
                self.table_placements[table_name] |= engine_bitvalue

                if orig != self.table_placements[table_name]:
                    table_diffs.append((table_name, self.table_placements[table_name]))

            except KeyError:
                # Some of the tables returned are not tables but names of CTEs.
                pass

        # Scan monetary costs that this query imposes.
        if location == Engine.Athena:
            self.athena_scanned_bytes += compute_athena_scanned_bytes(
                [query],
                [ctx.next_workload.get_predicted_athena_bytes_accessed(query_idx)],
                ctx.planner_config,
            )
        elif location == Engine.Aurora:
            self.aurora_accessed_pages += compute_aurora_accessed_pages(
                [query],
                [ctx.next_workload.get_predicted_aurora_pages_accessed(query_idx)],
            )

        self.workload_scan_cost = compute_athena_scan_cost(
            self.athena_scanned_bytes, ctx.planner_config
        ) + compute_aurora_scan_cost(
            self.aurora_accessed_pages,
            buffer_pool_hit_rate=ctx.metrics.buffer_hit_pct_avg / 100,
            planner_config=ctx.planner_config,
        )

        # Table movement costs that this query imposes.
        for name, next_placement in table_diffs:
            curr = ctx.current_blueprint.table_locations_bitmap()[name]
            if ((~curr) & next_placement) == 0:
                # This table was already present on the engine.
                continue

            result = compute_single_table_movement_time_and_cost(
                name, curr, next_placement, ctx
            )
            self.table_movement_trans_cost += result.movement_cost
            self.table_movement_trans_time_s += result.movement_time_s

            # If we added a table to Athena, we need to take into account its
            # storage costs.
            if (((~curr) & next_placement) & (EngineBitmapValues[Engine.Athena])) != 0:
                # We added the table to Athena.
                self.storage_cost += compute_single_athena_table_cost(name, ctx)

        # Adding a new query can affect the feasibility of the provisioning.
        self.feasibility = BlueprintFeasibility.Unchecked
        self.explored_provisionings = False
        self._memoized.clear()

    def get_all_query_indices(self) -> List[int]:
        return (
            self.query_locations[Engine.Aurora]
            + self.query_locations[Engine.Redshift]
            + self.query_locations[Engine.Athena]
        )

    def reset_routing(self) -> None:
        """
        This is used in the last step of the planner. We place queries during
        the optimization process using the learned predictions. In the last
        step, we want to re-route the queries using the router to re-score the
        final top k.
        """
        self.query_locations[Engine.Aurora].clear()
        self.query_locations[Engine.Redshift].clear()
        self.query_locations[Engine.Athena].clear()

        self.base_query_latencies[Engine.Aurora].clear()
        self.base_query_latencies[Engine.Redshift].clear()
        self.base_query_latencies[Engine.Athena].clear()

        self.scaled_query_latencies.clear()

        self.workload_scan_cost = 0.0
        self.athena_scanned_bytes = 0
        self.aurora_accessed_pages = 0

        self.feasibility = BlueprintFeasibility.Unchecked
        self._memoized.clear()

    def add_query_last_step(
        self,
        query_idx: int,
        query: Query,
        location: Engine,
        base_latency: float,
        ctx: ScoringContext,
    ) -> None:
        """
        This is used in the last step of the planner. We do not modify the table
        placement in this step. The query must be assigned to an engine that can
        support it.
        """
        self.query_locations[location].append(query_idx)
        self.base_query_latencies[location].append(base_latency)

        # Scan monetary costs that this query imposes.
        if location == Engine.Athena:
            self.athena_scanned_bytes += compute_athena_scanned_bytes(
                [query],
                [ctx.next_workload.get_predicted_athena_bytes_accessed(query_idx)],
                ctx.planner_config,
            )
        elif location == Engine.Aurora:
            self.aurora_accessed_pages += compute_aurora_accessed_pages(
                [query],
                [ctx.next_workload.get_predicted_aurora_pages_accessed(query_idx)],
            )

        self.workload_scan_cost = compute_athena_scan_cost(
            self.athena_scanned_bytes, ctx.planner_config
        ) + compute_aurora_scan_cost(
            self.aurora_accessed_pages,
            buffer_pool_hit_rate=ctx.metrics.buffer_hit_pct_avg / 100,
            planner_config=ctx.planner_config,
        )

    def add_transactional_tables(self, ctx: ScoringContext) -> None:
        referenced_tables = set()

        # Make sure that tables referenced in transactions are present on
        # Aurora.
        for query in ctx.next_workload.transactional_queries():
            for tbl in query.tables():
                if tbl not in self.table_placements:
                    # This is a CTE.
                    continue
                self.table_placements[tbl] |= EngineBitmapValues[Engine.Aurora]
                referenced_tables.add(tbl)

        # Update the table movement score if needed.
        for tbl in referenced_tables:
            cur = ctx.current_blueprint.table_locations_bitmap()[tbl]
            nxt = self.table_placements[tbl]
            if ((~cur) & nxt) == 0:
                continue

            result = compute_single_table_movement_time_and_cost(tbl, cur, nxt, ctx)
            self.table_movement_trans_cost += result.movement_cost
            self.table_movement_trans_time_s += result.movement_time_s

    def try_to_make_feasible_if_needed(self, ctx: ScoringContext) -> None:
        """
        Checks if this blueprint is already feasible, and if so, does nothing
        else. Otherwise, this method varies this blueprint's provisioning in the
        neighborhood to try and make it feasible.

        The idea is to avoid enumerating the provisioning if the blueprint is
        already feasible. This method is used as an intermediate during
        beam-based planning.
        """
        if not self.is_structurally_feasible():
            self.find_best_provisioning(ctx)

        else:
            # Already structurally feasible. Make sure it is also
            # "runtime-feasible".
            self.recompute_provisioning_dependent_scoring(ctx)
            self.compute_runtime_feasibility(ctx)
            if self.feasibility == BlueprintFeasibility.Infeasible:
                self.find_best_provisioning(ctx)

    def recompute_provisioning_dependent_scoring(self, ctx: ScoringContext) -> None:
        self._memoized.clear()
        aurora_prov_cost = compute_aurora_hourly_operational_cost(
            self.aurora_provisioning
        )
        redshift_prov_cost = compute_redshift_hourly_operational_cost(
            self.redshift_provisioning
        )

        aurora_transition_time_s = compute_aurora_transition_time_s(
            ctx.current_blueprint.aurora_provisioning(),
            self.aurora_provisioning,
            ctx.planner_config,
        )
        redshift_transition_time_s = compute_redshift_transition_time_s(
            ctx.current_blueprint.redshift_provisioning(),
            self.redshift_provisioning,
            ctx.planner_config,
        )

        self.provisioning_cost = aurora_prov_cost + redshift_prov_cost
        self.provisioning_trans_time_s = (
            aurora_transition_time_s + redshift_transition_time_s
        )

        self.aurora_score = AuroraProvisioningScore.compute(
            np.array(self.base_query_latencies[Engine.Aurora]),
            ctx.current_blueprint.aurora_provisioning(),
            self.aurora_provisioning,
            ctx,
        )

        self.scaled_query_latencies.clear()
        self.scaled_query_latencies[Engine.Aurora] = self.aurora_score.scaled_run_times
        self.scaled_query_latencies[Engine.Redshift] = scale_redshift_predicted_latency(
            np.array(self.base_query_latencies[Engine.Redshift]),
            self.redshift_provisioning,
            ctx,
        )

        # Account for load.
        if (
            ctx.current_blueprint.redshift_provisioning().num_nodes() > 0
            and self.redshift_provisioning.num_nodes() > 0
        ):
            self.redshift_cpu = compute_next_redshift_cpu(
                ctx.metrics.redshift_cpu_avg,
                ctx.current_blueprint.redshift_provisioning(),
                self.redshift_provisioning,
                self.scaled_query_latencies[Engine.Redshift].sum(),
                ctx,
            )
            self.scaled_query_latencies[
                Engine.Redshift
            ] = scale_redshift_run_time_by_load(
                self.scaled_query_latencies[Engine.Redshift], self.redshift_cpu, ctx
            )

    def is_better_than(self, other: "BlueprintCandidate") -> bool:
        return self._comparator(self, other)

    def __lt__(self, other: "BlueprintCandidate") -> bool:
        # This is implemented for use with Python's `heapq` module. It
        # implements a min-heap, but we want a max-heap.
        return not self.is_better_than(other)

    def find_best_provisioning(self, ctx: ScoringContext) -> None:
        """
        Tries all provisioinings in the neighborhood and finds the best
        scoring one for the current table placement and routing.
        """

        if self.explored_provisionings:
            # Already ran before.
            return

        aurora_enumerator = ProvisioningEnumerator(Engine.Aurora)
        aurora_it = aurora_enumerator.enumerate_nearby(
            ctx.current_blueprint.aurora_provisioning(),
            aurora_enumerator.scaling_to_distance(
                ctx.current_blueprint.aurora_provisioning(),
                ctx.planner_config.max_provisioning_multiplier(),
            ),
        )

        working_candidate = self.clone()
        current_best = None

        for aurora in aurora_it:
            working_candidate.update_aurora_provisioning(aurora)

            redshift_enumerator = ProvisioningEnumerator(Engine.Redshift)
            redshift_it = redshift_enumerator.enumerate_nearby(
                ctx.current_blueprint.redshift_provisioning(),
                redshift_enumerator.scaling_to_distance(
                    ctx.current_blueprint.redshift_provisioning(),
                    ctx.planner_config.max_provisioning_multiplier(),
                ),
            )

            for redshift in redshift_it:
                working_candidate.update_redshift_provisioning(redshift)
                if not working_candidate.is_structurally_feasible():
                    continue

                working_candidate.recompute_provisioning_dependent_scoring(ctx)
                working_candidate.compute_runtime_feasibility(ctx)
                if working_candidate.feasibility == BlueprintFeasibility.Infeasible:
                    continue

                if current_best is None:
                    current_best = working_candidate
                    working_candidate = working_candidate.clone()

                elif working_candidate.is_better_than(current_best):
                    current_best, working_candidate = working_candidate, current_best

        if (
            current_best is None
            or current_best.feasibility == BlueprintFeasibility.Infeasible
        ):
            self.feasibility = BlueprintFeasibility.Infeasible
            self.explored_provisionings = True
            return

        self.update_aurora_provisioning(current_best.aurora_provisioning)
        self.update_redshift_provisioning(current_best.redshift_provisioning)
        self.provisioning_cost = current_best.provisioning_cost
        self.provisioning_trans_time_s = current_best.provisioning_trans_time_s

        self.feasibility = current_best.feasibility
        self.explored_provisionings = True

    def is_structurally_feasible(self) -> bool:
        """
        Ensures that an engine is "on" if queries are assigned to it or if
        tables are placed on it.
        """
        if (
            len(self.base_query_latencies[Engine.Aurora]) > 0
            and self.aurora_provisioning.num_nodes() == 0
        ):
            return False

        if (
            len(self.base_query_latencies[Engine.Redshift]) > 0
            and self.redshift_provisioning.num_nodes() == 0
        ):
            return False

        # Make sure the provisioning supports the table placement.
        total_bitmap = 0
        for location_bitmap in self.table_placements.values():
            total_bitmap |= location_bitmap

        if (
            (EngineBitmapValues[Engine.Aurora] & total_bitmap) != 0
        ) and self.aurora_provisioning.num_nodes() == 0:
            # Aurora is turned off but we put tables on it.
            return False

        if (
            (EngineBitmapValues[Engine.Redshift] & total_bitmap) != 0
        ) and self.redshift_provisioning.num_nodes() == 0:
            # Redshift is turned off but we put tables on it.
            return False

        return True

    def compute_runtime_feasibility(self, ctx: ScoringContext) -> None:
        if self.feasibility != BlueprintFeasibility.Unchecked:
            # Already ran.
            return

        if self.aurora_score is not None:
            # TODO: Check whether there are transactions or not.
            if (
                self.aurora_score.overall_cpu_denorm
                >= self.aurora_score.pred_txn_peak_cpu_denorm
            ):
                self.feasibility = BlueprintFeasibility.Infeasible
            return

        if (
            not math.isnan(self.redshift_cpu)
            and self.redshift_cpu >= ctx.planner_config.max_feasible_cpu()
        ):
            self.feasibility = BlueprintFeasibility.Infeasible
            return

        self.feasibility = BlueprintFeasibility.Feasible

    def update_aurora_provisioning(self, prov: Provisioning) -> None:
        self.aurora_provisioning.set_instance_type(prov.instance_type())
        self.aurora_provisioning.set_num_nodes(prov.num_nodes())
        self.feasibility = BlueprintFeasibility.Unchecked
        self._memoized.clear()

    def update_redshift_provisioning(self, prov: Provisioning) -> None:
        self.redshift_provisioning.set_instance_type(prov.instance_type())
        self.redshift_provisioning.set_num_nodes(prov.num_nodes())
        self.feasibility = BlueprintFeasibility.Unchecked
        self._memoized.clear()

    def clone(self) -> "BlueprintCandidate":
        cloned = BlueprintCandidate(
            self._source_blueprint,
            self.aurora_provisioning.mutable_clone(),
            self.redshift_provisioning.mutable_clone(),
            self.table_placements.copy(),
            self._comparator,
        )

        for engine, indices in self.query_locations.items():
            cloned.query_locations[engine].extend(indices)
        for engine, lats in self.base_query_latencies.items():
            cloned.base_query_latencies[engine].extend(lats)

        cloned.provisioning_cost = self.provisioning_cost
        cloned.storage_cost = self.storage_cost
        cloned.workload_scan_cost = self.workload_scan_cost
        cloned.athena_scanned_bytes = self.athena_scanned_bytes
        cloned.aurora_accessed_pages = self.aurora_accessed_pages
        cloned.table_movement_trans_cost = self.table_movement_trans_cost

        cloned.table_movement_trans_time_s = self.table_movement_trans_time_s
        cloned.provisioning_trans_time_s = self.provisioning_trans_time_s

        cloned.explored_provisionings = self.explored_provisionings
        cloned.feasibility = self.feasibility
        cloned.redshift_cpu = self.redshift_cpu
        cloned.aurora_score = (
            self.aurora_score.copy() if self.aurora_score is not None else None
        )
        cloned.scaled_query_latencies = self.scaled_query_latencies.copy()
        # pylint: disable-next=protected-access
        cloned._memoized = self._memoized.copy()

        return cloned

    # `ComparableBlueprint` methods follow.

    def get_table_placement(self) -> Dict[str, List[Engine]]:
        placements = {}
        for name, bitmap in self.table_placements.items():
            placements[name] = Engine.from_bitmap(bitmap)
        return placements

    def get_aurora_provisioning(self) -> Provisioning:
        return self.aurora_provisioning

    def get_redshift_provisioning(self) -> Provisioning:
        return self.redshift_provisioning

    def get_predicted_analytical_latencies(self) -> npt.NDArray:
        relevant = []
        relevant.append(self.scaled_query_latencies[Engine.Aurora])
        relevant.append(self.scaled_query_latencies[Engine.Redshift])
        relevant.append(np.array(self.base_query_latencies[Engine.Athena]))
        return np.concatenate(relevant)

    def get_operational_monetary_cost(self) -> float:
        return self.storage_cost + self.provisioning_cost + self.workload_scan_cost

    def get_transition_cost(self) -> float:
        return self.table_movement_trans_cost

    def get_transition_time_s(self) -> float:
        return self.table_movement_trans_time_s + self.provisioning_trans_time_s

    def set_memoized_value(self, key: str, value: Any) -> None:
        self._memoized[key] = value

    def get_memoized_value(self, key: str) -> Optional[Any]:
        try:
            return self._memoized[key]
        except KeyError:
            return None
