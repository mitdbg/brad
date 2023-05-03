from typing import Dict, List, Tuple, Optional, Iterable
from itertools import chain
from pathlib import Path

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.planner.workload.query import Query
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

    @classmethod
    def from_extracted_logs(cls, file_path: str) -> "Workload":
        """
        Constructs a workload from extracted query logs. This method does not
        set the dataset size. Useful for testing purposes.
        """
        path = Path(file_path)

        txn_queries = []
        analytical_queries = []

        with open(path / "oltp.sql", encoding="UTF-8") as txns:
            for txn in txns:
                if txn.startswith("COMMIT"):
                    continue
                txn_queries.append(Query(txn))

        with open(path / "olap.sql", encoding="UTF-8") as analytics:
            for q in analytics:
                analytical_queries.append(Query(q))

        with open(path / "sample_prob.txt", encoding="UTF-8") as sample_file:
            sampling_prob = float(sample_file.read().strip())

        return cls(analytical_queries, txn_queries, sampling_prob, 0)

    def __init__(
        self,
        analytical_queries: List[Query],
        transactional_queries: List[Query],
        transaction_sample_fraction: float,
        dataset_size_mb: int,
    ) -> None:
        self._analytical_queries: List[Query] = analytical_queries
        self._transactional_queries: List[Query] = transactional_queries
        self._transaction_sample_fraction = transaction_sample_fraction
        self._dataset_size_mb = dataset_size_mb

        # The size of a table on an engine.
        self._table_sizes_mb: Dict[Tuple[str, Engine], int] = {}
        self._aurora_row_size_bytes: Dict[str, int] = {}

    def analytical_queries(self) -> List[Query]:
        return self._analytical_queries

    def transactional_queries(self) -> List[Query]:
        return self._transactional_queries

    def all_queries(self) -> Iterable[Query]:
        return chain(self._transactional_queries, self._analytical_queries)

    # TODO: Table size information should be put in a catalog class.

    def aurora_row_size_bytes(self, table_name: str) -> Optional[int]:
        try:
            return self._aurora_row_size_bytes[table_name]
        except KeyError:
            return None

    def table_sizes_empty(self) -> bool:
        return not self._table_sizes_mb

    def dataset_size_mb(self) -> int:
        return self._dataset_size_mb

    async def populate_table_sizes_using_blueprint(
        self, blueprint: Blueprint, table_sizer: TableSizer
    ) -> None:
        self._table_sizes_mb.clear()
        for table, locations in blueprint.tables_with_locations():
            for loc in locations:
                self._table_sizes_mb[
                    (table.name, loc)
                ] = await table_sizer.table_size_mb(table.name, loc)

            # Fetch the row size as well, if applicable.
            if Engine.Aurora in locations:
                self._aurora_row_size_bytes[
                    table.name
                ] = await table_sizer.aurora_row_size_bytes(table.name)

    def set_dataset_size_from_table_sizes(self) -> None:
        largest_table_mb: Dict[str, int] = {}
        for (table_name, _), size_mb in self._table_sizes_mb.items():
            if table_name not in largest_table_mb:
                largest_table_mb[table_name] = size_mb
            elif size_mb > largest_table_mb[table_name]:
                largest_table_mb[table_name] = size_mb

        self._dataset_size_mb = sum(largest_table_mb.values())

    def table_size_on_engine(self, table_name: str, location: Engine) -> Optional[int]:
        try:
            return self._table_sizes_mb[(table_name, location)]
        except KeyError:
            return None
