from typing import List

from brad.config.engine import Engine
from brad.query_rep import QueryRep
from brad.routing.context import RoutingContext
from brad.routing.abstract_policy import AbstractRoutingPolicy


class AlwaysOneRouter(AbstractRoutingPolicy):
    """
    This router always selects the same database engine for all queries.
    This router is useful for testing and benchmarking purposes.
    """

    def __init__(self, db_type: Engine):
        super().__init__()
        self._engine = db_type
        self._always_route_to = [db_type]

    def name(self) -> str:
        return f"AlwaysRouteTo({self._engine.name})"

    def engine_for_sync(self, _query: QueryRep, _ctx: RoutingContext) -> List[Engine]:
        return self._always_route_to

    def __eq__(self, other: object) -> bool:
        return isinstance(other, AlwaysOneRouter) and self._engine == other._engine
