from typing import Dict

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.query_rep import QueryRep
from brad.server.engine_connections import EngineConnections


class Query(QueryRep):
    """
    A `QueryRep` that is decorated with statistics that need to be obtained at
    runtime.
    """

    def __init__(self, sql_query: str):
        super().__init__(sql_query)
        self._data_accessed_mb: Dict[Engine, int] = {}
        self._tuples_accessed: Dict[Engine, int] = {}

    def data_accessed_mb(self, engine: Engine) -> int:
        return self._data_accessed_mb[engine]

    async def populate_data_accessed_mb(
        self, for_engine: Engine, connections: EngineConnections, blueprint: Blueprint
    ) -> None:
        if for_engine in self._data_accessed_mb:
            return

        # 1. See which engines hold all the tables involved in this query.
        locations = []
        for tbl in self.tables():
            locations.append(set(blueprint.get_table_locations(tbl)))
        candidate_engines = set.intersection(*locations)
        assert len(candidate_engines) > 0

        # We currently do not support Athena plans (engineering limitation).
        if len(candidate_engines) == 1 and Engine.Athena in candidate_engines:
            raise NotImplementedError(
                "Cannot yet extract the amount of data accessed from an Athena plan."
            )

        # TODO: Estimate the amount of data accessed by examining the base
        # operators when using EXPLAIN output.

    def clear_stats(self) -> None:
        self._data_accessed_mb.clear()
