import logging
import numpy as np
import numpy.typing as npt
from datetime import timedelta
from typing import Any, Dict, List, Optional, Iterable, Tuple

from brad.blueprint import Blueprint
from brad.blueprint.provisioning import Provisioning, MutableProvisioning
from brad.config.engine import Engine, EngineBitmapValues
from brad.planner.beam.feasibility import BlueprintFeasibility
from brad.planner.router_provider import RouterProvider
from brad.planner.compare.blueprint import ComparableBlueprint
from brad.planner.compare.function import BlueprintComparator
from brad.planner.enumeration.provisioning import ProvisioningEnumerator
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.performance.unified_aurora import AuroraProvisioningScore
from brad.planner.scoring.performance.unified_redshift import RedshiftProvisioningScore
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
from brad.planner.scoring.score import Score
from brad.planner.scoring.table_placement import (
    compute_single_athena_table_cost,
    compute_single_aurora_table_cost,
    compute_single_table_movement_time_and_cost,
)
from brad.routing.router import Router

logger = logging.getLogger(__name__)


class BlueprintCandidate(ComparableBlueprint):
    """
    A "barebones" representation of a blueprint, used during the table-based
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

        self.queries: List[int] = []

        self.query_locations: Dict[Engine, List[int]] = {}
        self.query_locations[Engine.Aurora] = []
        self.query_locations[Engine.Redshift] = []
        self.query_locations[Engine.Athena] = []

        self.scaled_query_latencies: Dict[Engine, npt.NDArray] = {}

        # Scoring components.

        # Monetary costs.
        self.provisioning_cost = 0.0
        self.storage_cost = 0.0
        self.workload_scan_cost = 0.0
        self.athena_scanned_bytes = 0
        self.aurora_accessed_pages = 0
        self.table_movement_trans_cost = 0.0

        # Transition times.
        self.table_movement_trans_time_s = 0.0
        self.provisioning_trans_time_s = 0.0

        # Used for scoring purposes.
        self.explored_provisionings = False
        self.feasibility = BlueprintFeasibility.Unchecked

        # Used during comparisons.
        self._memoized: Dict[str, Any] = {}
        self.aurora_score: Optional[AuroraProvisioningScore] = None
        self.redshift_score: Optional[RedshiftProvisioningScore] = None

    def to_blueprint(self) -> Blueprint:
        # We use the source blueprint for table schema information.
        return Blueprint(
            self._source_blueprint.schema_name(),
            self._source_blueprint.tables(),
            self.get_table_placement(),
            self.aurora_provisioning.clone(),
            self.redshift_provisioning.clone(),
            self._source_blueprint.get_routing_policy(),  # TODO: Use chosen policy.
        )

    def to_score(self) -> Score:
        score = Score()
        score.provisioning_cost = self.provisioning_cost
        score.storage_cost = self.storage_cost
        score.table_movement_trans_cost = self.table_movement_trans_cost

        score.workload_scan_cost = self.workload_scan_cost
        score.athena_scanned_bytes = self.athena_scanned_bytes
        score.aurora_accessed_pages = self.aurora_accessed_pages

        score.table_movement_trans_time_s = self.table_movement_trans_time_s
        score.provisioning_trans_time_s = self.provisioning_trans_time_s

        score.scaled_query_latencies = self.scaled_query_latencies
        score.aurora_score = self.aurora_score
        score.redshift_score = self.redshift_score

        score.aurora_queries = len(self.query_locations[Engine.Aurora])
        score.athena_queries = len(self.query_locations[Engine.Athena])
        score.redshift_queries = len(self.query_locations[Engine.Redshift])

        return score

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

        if self.aurora_score is not None:
            self.aurora_score.add_debug_values(values)

        if self.redshift_score is not None:
            self.redshift_score.add_debug_values(values)

        return values

    def add_placement(
        self, placement_bitmap: int, tables: Iterable[str], ctx: ScoringContext
    ) -> bool:
        """
        Returns `True` iff adding this placement changed the existing placement
        in any way.
        """
        changed = False
        changed_tables = []

        for table_name in tables:
            cur = self.table_placements[table_name]
            nxt = cur | placement_bitmap
            self.table_placements[table_name] = nxt
            if nxt != cur:
                changed_tables.append(table_name)
                changed = True

        # Update movement scoring.
        self._update_movement_score(changed_tables, ctx)

        self.feasibility = BlueprintFeasibility.Unchecked
        self.explored_provisionings = False
        self._memoized.clear()

        return changed

    async def add_query_cluster(
        self,
        router_provider: RouterProvider,
        query_cluster: List[int],
        reroute_prev: bool,
        ctx: ScoringContext,
    ) -> None:
        router: Router = await router_provider.get_router(self.table_placements)

        if reroute_prev:
            self.query_locations[Engine.Aurora].clear()
            self.query_locations[Engine.Redshift].clear()
            self.query_locations[Engine.Athena].clear()

            (
                dests,
                aurora_accessed_pages,
                athena_scanned_bytes,
            ) = await self._route_queries_compute_scan_stats(self.queries, router, ctx)
            for eng, query_indices in dests.items():
                self.query_locations[eng].extend(query_indices)

            self.aurora_accessed_pages = aurora_accessed_pages
            self.athena_scanned_bytes = athena_scanned_bytes

        (
            cluster_dests,
            incr_aurora_accessed_pages,
            incr_athena_scanned_bytes,
        ) = await self._route_queries_compute_scan_stats(query_cluster, router, ctx)
        for eng, query_indices in cluster_dests.items():
            self.query_locations[eng].extend(query_indices)

        self.aurora_accessed_pages += incr_aurora_accessed_pages
        self.athena_scanned_bytes += incr_athena_scanned_bytes

        self.workload_scan_cost = compute_athena_scan_cost(
            self.athena_scanned_bytes, ctx.planner_config
        )

        if not ctx.planner_config.use_io_optimized_aurora():
            has_read_replicas = (
                ctx.current_blueprint.aurora_provisioning().num_nodes() > 1
            )
            if has_read_replicas:
                hit_rate = ctx.metrics.aurora_reader_buffer_hit_pct_avg / 100.0
            else:
                hit_rate = ctx.metrics.aurora_writer_buffer_hit_pct_avg / 100.0

            self.workload_scan_cost += compute_aurora_scan_cost(
                self.aurora_accessed_pages,
                buffer_pool_hit_rate=hit_rate,
                planner_config=ctx.planner_config,
            )

        self.queries.extend(query_cluster)

        self.feasibility = BlueprintFeasibility.Unchecked
        self.explored_provisionings = False
        self._memoized.clear()

    async def _route_queries_compute_scan_stats(
        self,
        queries: List[int],
        router: Router,
        ctx: ScoringContext,
    ) -> Tuple[Dict[Engine, List[int]], int, int]:
        all_queries = ctx.next_workload.analytical_queries()
        dests: Dict[Engine, List[int]] = {
            Engine.Aurora: [],
            Engine.Redshift: [],
            Engine.Athena: [],
        }

        for qidx in queries:
            q = all_queries[qidx]
            eng = await router.engine_for(q)
            dests[eng].append(qidx)

        aurora_queries = [all_queries[qidx] for qidx in dests[Engine.Aurora]]
        aurora_accessed_pages = compute_aurora_accessed_pages(
            aurora_queries,
            ctx.next_workload.get_predicted_aurora_pages_accessed_batch(queries),
        )

        athena_queries = [all_queries[qidx] for qidx in dests[Engine.Athena]]
        athena_scanned_bytes = compute_athena_scanned_bytes(
            athena_queries,
            ctx.next_workload.get_predicted_athena_bytes_accessed_batch(
                dests[Engine.Athena]
            ),
            ctx.planner_config,
        )

        return dests, aurora_accessed_pages, athena_scanned_bytes

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

        # If we made a change to the table placement, see if it corresponds to a
        # table score change.
        self._update_movement_score(referenced_tables, ctx)

    def _update_movement_score(
        self, relevant_tables: Iterable[str], ctx: ScoringContext
    ) -> None:
        for tbl in relevant_tables:
            cur = ctx.current_blueprint.table_locations_bitmap()[tbl]
            nxt = self.table_placements[tbl]
            if ((~cur) & nxt) == 0:
                continue

            result = compute_single_table_movement_time_and_cost(tbl, cur, nxt, ctx)
            self.table_movement_trans_cost += result.movement_cost
            self.table_movement_trans_time_s += result.movement_time_s

            # If we added the table to Athena or Aurora, we need to take into
            # account its storage costs.
            if (((~cur) & nxt) & (EngineBitmapValues[Engine.Athena])) != 0:
                # We added the table to Athena.
                self.storage_cost += compute_single_athena_table_cost(tbl, ctx)

            if (((~cur) & nxt) & (EngineBitmapValues[Engine.Aurora])) != 0:
                # Added table to Aurora.
                # You only pay for 1 copy of the table on Aurora, regardless of
                # how many read replicas you have.
                self.storage_cost += compute_single_aurora_table_cost(tbl, ctx)

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

        # Provisioning costs.
        aurora_prov_cost = compute_aurora_hourly_operational_cost(
            self.aurora_provisioning, ctx
        )
        redshift_prov_cost = compute_redshift_hourly_operational_cost(
            self.redshift_provisioning
        )
        cost_scale_factor = timedelta(hours=1) / ctx.next_workload.period()

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

        self.provisioning_cost = (
            aurora_prov_cost + redshift_prov_cost
        ) * cost_scale_factor
        self.provisioning_trans_time_s = (
            aurora_transition_time_s + redshift_transition_time_s
        )

        self.aurora_score = AuroraProvisioningScore.compute(
            ctx.next_workload.get_predicted_analytical_latency_batch(
                self.query_locations[Engine.Aurora], Engine.Aurora
            ),
            ctx.next_workload.get_arrival_counts_batch(
                self.query_locations[Engine.Aurora]
            ),
            ctx.current_blueprint.aurora_provisioning(),
            self.aurora_provisioning,
            ctx,
        )
        self.redshift_score = RedshiftProvisioningScore.compute(
            ctx.next_workload.get_predicted_analytical_latency_batch(
                self.query_locations[Engine.Redshift], Engine.Redshift
            ),
            ctx.next_workload.get_arrival_counts_batch(
                self.query_locations[Engine.Redshift]
            ),
            ctx.current_blueprint.redshift_provisioning(),
            self.redshift_provisioning,
            ctx,
        )

        # Predicted query performance.
        self.scaled_query_latencies.clear()
        self.scaled_query_latencies[Engine.Aurora] = self.aurora_score.scaled_run_times
        self.scaled_query_latencies[
            Engine.Redshift
        ] = self.redshift_score.scaled_run_times
        self.scaled_query_latencies[
            Engine.Athena
        ] = ctx.next_workload.get_predicted_analytical_latency_batch(
            self.query_locations[Engine.Athena], Engine.Athena
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
                Engine.Aurora,
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
                    Engine.Redshift,
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
            return False

        if (
            (EngineBitmapValues[Engine.Redshift] & total_bitmap) != 0
        ) and self.redshift_provisioning.num_nodes() == 0:
            return False

        return True

    def compute_runtime_feasibility(self, _ctx: ScoringContext) -> None:
        if self.feasibility != BlueprintFeasibility.Unchecked:
            # Already ran.
            return

        # We used to check for transactional load here. Now it's scored as part
        # of performance.
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

        cloned.queries = self.queries.copy()

        cloned.provisioning_cost = self.provisioning_cost
        cloned.storage_cost = self.storage_cost
        cloned.workload_scan_cost = self.workload_scan_cost
        cloned.aurora_accessed_pages = self.aurora_accessed_pages
        cloned.athena_scanned_bytes = self.athena_scanned_bytes
        cloned.table_movement_trans_cost = self.table_movement_trans_cost

        cloned.table_movement_trans_time_s = self.table_movement_trans_time_s
        cloned.provisioning_trans_time_s = self.provisioning_trans_time_s

        cloned.explored_provisionings = self.explored_provisionings
        cloned.feasibility = self.feasibility
        cloned.scaled_query_latencies = self.scaled_query_latencies.copy()
        cloned.aurora_score = (
            self.aurora_score.copy() if self.aurora_score is not None else None
        )
        cloned.redshift_score = (
            self.redshift_score.copy() if self.redshift_score is not None else None
        )
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
        relevant.append(self.scaled_query_latencies[Engine.Athena])
        return np.concatenate(relevant)

    def get_predicted_transactional_latencies(self) -> npt.NDArray:
        assert self.aurora_score is not None
        return self.aurora_score.scaled_txn_lats

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

    def __getstate__(self) -> Dict[Any, Any]:
        # This is used for debug logging purposes.
        copied = self.__dict__.copy()
        # This is not serializable, nor do we need it to be (for debug purposes).
        copied["_comparator"] = None
        return copied

    def __setstate__(self, d: Dict[Any, Any]) -> None:
        self.__dict__ = d
        logger.info("Note: Deserializing table-based blueprint candidate.")
