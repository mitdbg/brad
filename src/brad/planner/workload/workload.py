from typing import Dict, List, Tuple, Optional, Iterable
from itertools import chain

from brad.blueprint import Blueprint
from brad.query_rep import QueryRep
from brad.config.engine import Engine
from brad.utils.table_sizer import TableSizer


class Workload:
    """
    A representation of the workload to be considered during the blueprint
    planning process.

    - List of analytical queries
    - List of transactions (sampled) and the sample frequency
    - Total dataset size
    - Table sizes

    The planner uses these values when comparing blueprints. The intention is
    that these values can also be _forecasted_.
    """

    @classmethod
    def empty(cls) -> "Workload":
        return cls([], [], 0.01, 0)

    def __init__(
        self,
        analytical_queries: List[QueryRep],
        transactional_queries: List[QueryRep],
        transaction_sample_fraction: float,
        dataset_size_mb: int,
    ) -> None:
        self._analytical_queries: List[QueryRep] = analytical_queries
        self._transactional_queries: List[QueryRep] = transactional_queries
        self._transaction_sample_fraction = transaction_sample_fraction
        self._dataset_size_mb = dataset_size_mb

        # The size of a table on an engine.
        self._table_sizes_mb: Dict[Tuple[str, Engine], int] = {}

    def analytical_queries(self) -> List[QueryRep]:
        return self._analytical_queries

    def transactional_queries(self) -> List[QueryRep]:
        return self._transactional_queries

    def all_queries(self) -> Iterable[QueryRep]:
        return chain(self._transactional_queries, self._analytical_queries)

    async def populate_table_sizes_using_blueprint(
        self, blueprint: Blueprint, table_sizer: TableSizer
    ) -> None:
        self._table_sizes_mb.clear()
        for table, locations in blueprint.tables_with_locations():
            for loc in locations:
                self._table_sizes_mb[
                    (table.name, loc)
                ] = await table_sizer.table_size_mb(table.name, loc)

    def table_size_on_engine(self, table_name: str, location: Engine) -> Optional[int]:
        try:
            return self._table_sizes_mb[(table_name, location)]
        except KeyError:
            return None
