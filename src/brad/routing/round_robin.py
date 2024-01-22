from typing import List

from brad.config.engine import Engine
from brad.query_rep import QueryRep
from brad.routing.abstract_policy import AbstractRoutingPolicy
from brad.routing.context import RoutingContext


class RoundRobin(AbstractRoutingPolicy):
    """
    Routes queries in a round-robin fashion.
    """

    def __init__(self):
        self._ordering = [Engine.Athena, Engine.Aurora, Engine.Redshift]

    def name(self) -> str:
        return "RoundRobin"

    def engine_for_sync(self, _query: QueryRep, _ctx: RoutingContext) -> List[Engine]:
        tmp = self._ordering[0]
        self._ordering[0] = self._ordering[1]
        self._ordering[1] = self._ordering[2]
        self._ordering[2] = tmp
        return self._ordering

    def __eq__(self, other: object) -> bool:
        return isinstance(other, RoundRobin)
