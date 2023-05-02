import logging
from typing import Dict

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.query_rep import QueryRep
from brad.server.engine_connections import EngineConnections
from brad.planner.plan_parsing import parse_explain_verbose, extract_base_cardinalities

logger = logging.getLogger(__name__)


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

        # 2. Choose the source engine.
        if for_engine in candidate_engines and for_engine != Engine.Athena:
            source_engine = for_engine
        else:
            source_engine = (
                Engine.Aurora if Engine.Aurora in candidate_engines else Engine.Redshift
            )

        if source_engine == Engine.Aurora:
            query = "EXPLAIN VERBOSE {}".format(self.raw_query)
            aurora = connections.get_connection(Engine.Aurora)
            cursor = await aurora.cursor()
            logger.debug("Running on Aurora: %s", query)
        else:
            assert source_engine == Engine.Redshift
            query = "EXPLAIN {}".format(self.raw_query)
            redshift = connections.get_connection(Engine.Redshift)
            cursor = await redshift.cursor()
            logger.debug("Running on Redshift: %s", query)

        await cursor.execute(query)
        plan_rows = [row async for row in cursor]
        plan = parse_explain_verbose(plan_rows)
        base_cardinalities = extract_base_cardinalities(plan)

        # 3. Sum up the results.
        # NOTE: This approach is not entirely correct. It is written this way
        # for convenience to get started, but will be fixed later.
        #
        # Aurora storage is row-based, so we should be adding the size of the
        # entire row (not the reported width). This approach underestimates how
        # much data it reads.
        #
        # Redshift and Athena storage (Iceberg using Parquet) is column-based
        # and uses compression. This approach overestimates the amount of data
        # it reads.
        total_storage_bytes = 0
        for bc in base_cardinalities:
            total_storage_bytes += bc.cardinality * bc.width

        # MB, so we divide by 1000 twice.
        self._data_accessed_mb[for_engine] = total_storage_bytes // 1000 // 1000

    def clear_stats(self) -> None:
        self._data_accessed_mb.clear()
