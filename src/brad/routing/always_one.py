from brad.routing import Router
from brad.config.engine import Engine
from brad.query_rep import QueryRep


class AlwaysOneRouter(Router):
    """
    This router always selects the same database engine for all queries.
    This router is useful for testing and benchmarking purposes.
    """

    def __init__(self, db_type: Engine):
        super().__init__()
        self._always_route_to = db_type

    def engine_for_sync(self, _query: QueryRep) -> Engine:
        return self._always_route_to
