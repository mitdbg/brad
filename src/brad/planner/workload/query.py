from typing import Dict

from brad.config.engine import Engine
from brad.query_rep import QueryRep
from brad.server.engine_connections import EngineConnections


class Query(QueryRep):
    """
    A `QueryRep` that is decorated with statistics that we can obtain from
    EXPLAIN.
    """

    def __init__(self, sql_query: str):
        super().__init__(sql_query)
        self._data_accessed_mb: Dict[Engine, int] = {}

    def data_accessed_mb(self, engine: Engine) -> int:
        return self._data_accessed_mb[engine]

    async def populate_data_accessed_mb(
        self, for_engine: Engine, _connections: EngineConnections
    ) -> None:
        if for_engine in self._data_accessed_mb:
            return

        # TODO: Estimate the amount of data accessed by examining the base
        # operators when using EXPLAIN output.
