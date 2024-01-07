import logging
import numpy as np
import numpy.typing as npt
from datetime import timedelta
from typing import Any, Dict, List, Optional

from brad.blueprint import Blueprint
from brad.blueprint.provisioning import Provisioning
from brad.config.engine import Engine, EngineBitmapValues
from brad.planner.compare.blueprint import ComparableBlueprint
from brad.planner.compare.function import BlueprintComparator
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.performance.unified_aurora import AuroraProvisioningScore
from brad.planner.scoring.performance.unified_redshift import RedshiftProvisioningScore
from brad.planner.scoring.provisioning import (
    compute_aurora_hourly_operational_cost,
    compute_redshift_hourly_operational_cost,
    compute_athena_scan_cost,
    compute_athena_scanned_bytes_batch,
    compute_aurora_transition_time_s,
    compute_redshift_transition_time_s,
)
from brad.planner.scoring.score import Score
from brad.planner.scoring.table_placement import (
    compute_single_athena_table_cost,
    compute_single_aurora_table_cost,
    compute_single_table_movement_time_and_cost,
)
from brad.planner.workload import Workload
from brad.planner.workload.query import Query
from brad.routing.abstract_policy import FullRoutingPolicy
from brad.routing.cached import CachedLocationPolicy

logger = logging.getLogger(__name__)


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
            blueprint.aurora_provisioning().clone(),
            blueprint.redshift_provisioning().clone(),
            {t.name: 0 for t in blueprint.tables()},
            comparator,
        )

    def __init__(
        self,
        source: Blueprint,
        aurora: Provisioning,
        redshift: Provisioning,
        table_placements: Dict[str, int],
        comparator: BlueprintComparator,
    ) -> None:
        self.aurora_provisioning = aurora
        self.redshift_provisioning = redshift
        # Table locations are represented using a bitmap. We initialize each
        # table to being present on no engines.
        self.table_placements = table_placements

        self._source_blueprint = source
        self._comparator = comparator

        self.query_locations: Dict[Engine, List[int]] = {}
        self.query_locations[Engine.Aurora] = []
        self.query_locations[Engine.Redshift] = []
        self.query_locations[Engine.Athena] = []

        self.score = Score()
        self._memoized: Dict[str, int | float | str] = {}

    def add_query(
        self,
        query_idx: int,
        query: Query,
        location: Engine,
    ) -> None:
        self.query_locations[location].append(query_idx)
        engine_bitvalue = EngineBitmapValues[location]

        # Ensure that the table is present on the engine on which we want to run
        # the query.
        table_diffs = []
        for table_name in query.tables():
            try:
                orig = self.table_placements[table_name]
                self.table_placements[table_name] |= engine_bitvalue

                if orig != self.table_placements[table_name]:
                    table_diffs.append(
                        (table_name, self.table_placements[table_name], orig),
                    )

            except KeyError:
                # Some of the tables returned are not tables but names of CTEs.
                pass

    def compute_score(self, ctx: ScoringContext) -> None:
        score = self.score

        # Provisioning.
        aurora_prov_cost = compute_aurora_hourly_operational_cost(
            self.aurora_provisioning, ctx
        )
        redshift_prov_cost = compute_redshift_hourly_operational_cost(
            self.redshift_provisioning
        )
        cost_scale_factor = timedelta(hours=1) / ctx.next_workload.period()

        # Transition times.
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

        score.provisioning_cost = (
            aurora_prov_cost + redshift_prov_cost
        ) * cost_scale_factor
        score.provisioning_trans_time_s = (
            aurora_transition_time_s + redshift_transition_time_s
        )

        # Performance.
        score.aurora_score = AuroraProvisioningScore.compute(
            self.query_locations[Engine.Aurora],
            ctx.next_workload,
            ctx.current_blueprint.aurora_provisioning(),
            self.aurora_provisioning,
            ctx,
        )
        score.redshift_score = RedshiftProvisioningScore.compute(
            self.query_locations[Engine.Redshift],
            ctx.next_workload,
            ctx.current_blueprint.redshift_provisioning(),
            self.redshift_provisioning,
            ctx,
        )
        score.scaled_query_latencies.clear()
        score.scaled_query_latencies[
            Engine.Aurora
        ] = score.aurora_score.scaled_run_times
        score.scaled_query_latencies[
            Engine.Redshift
        ] = score.redshift_score.scaled_run_times
        score.scaled_query_latencies[
            Engine.Athena
        ] = ctx.next_workload.get_predicted_analytical_latency_batch(
            self.query_locations[Engine.Athena], Engine.Athena
        )

        score.aurora_queries = len(self.query_locations[Engine.Aurora])
        score.athena_queries = len(self.query_locations[Engine.Athena])
        score.redshift_queries = len(self.query_locations[Engine.Redshift])

        # Scan costs.
        score.athena_scanned_bytes = compute_athena_scanned_bytes_batch(
            ctx.next_workload.get_predicted_athena_bytes_accessed_batch(
                self.query_locations[Engine.Athena]
            ),
            ctx.next_workload.get_arrival_counts_batch(
                self.query_locations[Engine.Athena]
            ),
            ctx.planner_config,
        )
        score.workload_scan_cost = compute_athena_scan_cost(
            score.athena_scanned_bytes, ctx.planner_config
        )

        # Table movement and storage.
        score.storage_cost = 0.0
        score.table_movement_trans_cost = 0.0
        score.table_movement_trans_time_s = 0.0
        for table_name, placement_bitmap in self.table_placements.items():
            # If we added a table to Athena or Aurora, we need to take into
            # account its storage costs.
            if (placement_bitmap & EngineBitmapValues[Engine.Athena]) != 0:
                # Table is on Athena.
                score.storage_cost += compute_single_athena_table_cost(table_name, ctx)

            if (placement_bitmap & EngineBitmapValues[Engine.Aurora]) != 0:
                # Table is on Aurora.
                # You only pay for 1 copy of the table on Aurora, regardless of
                # how many read replicas you have.
                score.storage_cost += compute_single_aurora_table_cost(table_name, ctx)

            curr = ctx.current_blueprint.table_locations_bitmap()[table_name]
            if ((~curr) & placement_bitmap) == 0:
                # This table was already present on the relevant engines.
                continue

            result = compute_single_table_movement_time_and_cost(
                table_name, curr, placement_bitmap, ctx
            )
            score.table_movement_trans_cost += result.movement_cost
            score.table_movement_trans_time_s += result.movement_time_s

    def get_all_query_indices(self) -> List[int]:
        return (
            self.query_locations[Engine.Aurora]
            + self.query_locations[Engine.Redshift]
            + self.query_locations[Engine.Athena]
        )

    def add_transactional_tables(self, ctx: ScoringContext) -> None:
        # Make sure that tables referenced in transactions are present on
        # Aurora.
        for query in ctx.next_workload.transactional_queries():
            for tbl in query.tables():
                if tbl not in self.table_placements:
                    # This is a CTE.
                    continue
                self.table_placements[tbl] |= EngineBitmapValues[Engine.Aurora]

    def is_better_than(self, other: "BlueprintCandidate") -> bool:
        return self._comparator(self, other)

    def __lt__(self, other: "BlueprintCandidate") -> bool:
        # This is implemented for use with Python's `heapq` module. It
        # implements a min-heap, but we want a max-heap.
        return not self.is_better_than(other)

    def is_structurally_feasible(self) -> bool:
        """
        Ensures that an engine is "on" if queries are assigned to it or if
        tables are placed on it.
        """
        if (
            len(self.query_locations[Engine.Aurora]) > 0
            and self.aurora_provisioning.num_nodes() == 0
        ):
            return False

        if (
            len(self.query_locations[Engine.Redshift]) > 0
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
        return cloned

    def to_blueprint(self, ctx: ScoringContext) -> Blueprint:
        # We should cache the routing locations chosen by the planner.
        cached_policy = CachedLocationPolicy.from_planner(
            ctx.next_workload, self.query_locations
        )
        current_policy = self._source_blueprint.get_routing_policy()
        routing_policy = FullRoutingPolicy(
            indefinite_policies=[cached_policy],
            # We re-use the same definite policy.
            definite_policy=current_policy.definite_policy,
        )
        # We use the source blueprint for table schema information.
        return Blueprint(
            self._source_blueprint.schema_name(),
            self._source_blueprint.tables(),
            self.get_table_placement(),
            self.aurora_provisioning.clone(),
            self.redshift_provisioning.clone(),
            routing_policy,
        )

    def to_debug_values(self) -> Dict[str, int | float | str]:
        values: Dict[str, int | float | str] = self._memoized.copy()

        # Provisioning.
        values["aurora_instance"] = self.aurora_provisioning.instance_type()
        values["aurora_nodes"] = self.aurora_provisioning.num_nodes()
        values["redshift_instance"] = self.redshift_provisioning.instance_type()
        values["redshift_nodes"] = self.redshift_provisioning.num_nodes()

        # Query breakdowns (rough).
        values["aurora_queries"] = len(self.query_locations[Engine.Aurora])
        values["redshift_queries"] = len(self.query_locations[Engine.Redshift])
        values["athena_queries"] = len(self.query_locations[Engine.Athena])

        # Scoring components.
        values["provisioning_cost"] = self.score.provisioning_cost
        values["storage_cost"] = self.score.storage_cost
        values["workload_scan_cost"] = self.score.workload_scan_cost
        values["athena_scanned_bytes"] = self.score.athena_scanned_bytes
        values["aurora_accessed_pages"] = self.score.aurora_accessed_pages
        values["table_movement_trans_cost"] = self.score.table_movement_trans_cost
        values["table_movement_trans_time_s"] = self.score.table_movement_trans_time_s
        values["provisioning_trans_time_s"] = self.score.provisioning_trans_time_s

        if self.score.aurora_score is not None:
            self.score.aurora_score.add_debug_values(values)

        if self.score.redshift_score is not None:
            self.score.redshift_score.add_debug_values(values)

        return values

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

    def get_routing_decisions(self) -> npt.NDArray:
        engine_query = np.array(
            [
                (Workload.EngineLatencyIndex[engine], idx)
                for engine, qidxs in self.query_locations.items()
                for idx in qidxs
            ]
        )
        si = np.argsort(engine_query[:, 1])
        sorted_queries = engine_query[si]
        return sorted_queries[:, 0]

    def get_predicted_analytical_latencies(self) -> npt.NDArray:
        relevant = []
        relevant.append(self.score.scaled_query_latencies[Engine.Aurora])
        relevant.append(self.score.scaled_query_latencies[Engine.Redshift])
        relevant.append(self.score.scaled_query_latencies[Engine.Athena])
        return np.concatenate(relevant)

    def get_predicted_transactional_latencies(self) -> npt.NDArray:
        assert self.score.aurora_score is not None
        return self.score.aurora_score.scaled_txn_lats

    def get_operational_monetary_cost(self) -> float:
        return (
            self.score.storage_cost
            + self.score.provisioning_cost
            + self.score.workload_scan_cost
        )

    def get_transition_cost(self) -> float:
        return self.score.table_movement_trans_cost

    def get_transition_time_s(self) -> float:
        return (
            self.score.table_movement_trans_time_s
            + self.score.provisioning_trans_time_s
        )

    def set_memoized_value(self, key: str, value: Any) -> None:
        self._memoized[key] = value

    def get_memoized_value(self, key: str) -> Optional[Any]:
        try:
            return self._memoized[key]
        except KeyError:
            return None

    def __getstate__(self) -> Dict[Any, Any]:
        # This is used for debug logging purposes.
        copied = self.__dict__.copy()
        # This is not serializable, nor do we need it to be (for debug purposes).
        copied["_comparator"] = None
        return copied

    def __setstate__(self, d: Dict[Any, Any]) -> None:
        self.__dict__ = d
        logger.info("Note: Deserializing FPQB blueprint candidate.")
