from typing import List, Dict

from brad.config.engine import Engine
from brad.planner.workload import Workload
from brad.query_rep import QueryRep
from brad.routing.abstract_policy import AbstractRoutingPolicy
from brad.routing.context import RoutingContext


class CachedLocationPolicy(AbstractRoutingPolicy):
    """
    This is an indefinite routing policy, which means `engine_for()` may return
    an empty list.
    """

    @classmethod
    def from_planner(
        cls, workload: Workload, chosen_locations: Dict[Engine, List[int]]
    ) -> "CachedLocationPolicy":
        queries = workload.analytical_queries()
        query_map = {}
        for engine, query_idxs in chosen_locations.items():
            for qidx in query_idxs:
                qrep = queries[qidx].copy_as_query_rep()
                query_map[qrep] = engine
        return cls(query_map)

    def __init__(self, query_map: Dict[QueryRep, Engine]) -> None:
        self._query_map = query_map

    def name(self) -> str:
        return "CachedLocationPolicy"

    def engine_for_sync(
        self, query_rep: QueryRep, _ctx: RoutingContext
    ) -> List[Engine]:
        try:
            return [self._query_map[query_rep]]
        except KeyError:
            return []

    def __repr__(self) -> str:
        # Summarize the cached routing state.
        routing_count: Dict[Engine, int] = {
            Engine.Aurora: 0,
            Engine.Redshift: 0,
            Engine.Athena: 0,
        }
        for engine in self._query_map.values():
            routing_count[engine] += 1
        return (
            f"CachedLocations(Aurora={routing_count[Engine.Aurora]}, "
            f"Redshift={routing_count[Engine.Redshift]}, "
            f"Athena={routing_count[Engine.Athena]})"
        )
