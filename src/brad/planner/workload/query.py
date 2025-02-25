import logging
from datetime import datetime
from collections import Counter
from typing import Dict, List, Tuple, Optional
from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.query_rep import QueryRep
from brad.data_stats.plan_parsing import (
    parse_explain_verbose,
    extract_base_cardinalities,
)
from brad.front_end.engine_connections import EngineConnections

logger = logging.getLogger(__name__)


class Query(QueryRep):
    """
    A `QueryRep` that is decorated with additional statistics that are used for
    blueprint planning.

    Objects of this class are logically immutable.
    """

    def __init__(
        self,
        sql_query: str,
        arrival_count: float = 1.0,
        past_executions: Optional[List[Tuple[Engine, float, datetime]]] = None,
    ):
        super().__init__(sql_query)
        self._arrival_count = arrival_count
        self._past_executions = past_executions
        # `arrival_count` might be scaled for a fixed period. This multiplier
        # represents the scaling factor used; it should be applied to
        # `past_executions` when reweighing a query distribution.
        self._past_executions_multiplier = 1.0

        # Legacy statistics.
        self._data_accessed_mb: Dict[Engine, int] = {}
        self._tuples_accessed: Dict[Engine, int] = {}

    def copy_as_query_rep(self) -> QueryRep:
        return QueryRep(self.raw_query)

    def past_executions(self) -> Optional[List[Tuple[Engine, float, datetime]]]:
        """
        Retrieve any information about past executions of this query.
        """
        return self._past_executions

    def arrival_count(self) -> float:
        """
        Note that this value may be fractional due to time period adjustments in
        the workload.
        """
        return self._arrival_count

    def set_arrival_count(self, arrival_count: float) -> None:
        self._arrival_count = arrival_count

    def set_past_executions_multiplier(self, multiplier: float) -> None:
        self._past_executions_multiplier = multiplier

    # The methods below are legacy code.

    def data_accessed_mb(self, engine: Engine) -> int:
        return self._data_accessed_mb[engine]

    def populate_data_accessed_mb(
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
            cursor = aurora.cursor_sync()
        else:
            assert source_engine == Engine.Redshift
            query = "EXPLAIN {}".format(self.raw_query)
            redshift = connections.get_connection(Engine.Redshift)
            cursor = redshift.cursor_sync()

        cursor.execute_sync(query)
        plan_rows = [tuple(row) for row in cursor]
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

    def most_common_execution_location(self) -> Optional[Engine]:
        # NOTE: This may capture routing decisions made in a previous blueprint
        # if your planning window spans multiple previous blueprints. Depending
        # on your needs, `most_recent_execution_location()` might be better.
        if self._past_executions is None or len(self._past_executions) == 0:
            return None

        counter = Counter([execution[0] for execution in self._past_executions])
        most_common, _ = counter.most_common(1)[0]
        return most_common

    def most_recent_execution_location(self) -> Optional[Engine]:
        if self._past_executions is None or len(self._past_executions) == 0:
            return None

        latest_epoch = None
        latest_idx = None

        for idx, execution in enumerate(self._past_executions):
            if latest_epoch is None:
                latest_epoch = execution[2]
                latest_idx = idx
            elif execution[2] > latest_epoch:
                latest_epoch = execution[2]
                latest_idx = idx

        assert latest_idx is not None
        return self._past_executions[latest_idx][0]

    def most_recent_execution(self) -> Optional[Tuple[Engine, float]]:
        if self._past_executions is None or len(self._past_executions) == 0:
            return None

        latest_epoch = None
        latest_idx = None

        for idx, execution in enumerate(self._past_executions):
            if latest_epoch is None:
                latest_epoch = execution[2]
                latest_idx = idx
            elif execution[2] > latest_epoch:
                latest_epoch = execution[2]
                latest_idx = idx

        assert latest_idx is not None
        return (
            self._past_executions[latest_idx][0],
            self._past_executions[latest_idx][1],
        )
