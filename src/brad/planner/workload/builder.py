import pathlib
from datetime import timedelta
from typing import List, Dict, Optional

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.planner.workload import Workload
from brad.planner.workload.query import Query
from brad.utils.table_sizer import TableSizer


class WorkloadBuilder:
    """
    Helps create custom workloads, useful for experiments and debugging
    purposes.

    This builder only supports expressing uniform arrivals across all queries in
    the workload.
    """

    def __init__(self) -> None:
        self._analytical_queries: List[str] = []
        self._transactional_queries: List[str] = []
        self._analytics_count_per: int = 1
        self._total_transaction_count: int = 0
        self._period = timedelta(hours=1)
        self._table_sizes: Dict[str, int] = {}

    def build(self) -> Workload:
        analytics = [
            Query(q, arrival_count=self._analytics_count_per * count)
            for q, count in self._deduplicate_queries(self._analytical_queries).items()
        ]
        transactions = [
            Query(q, arrival_count=0)
            for q in self._deduplicate_queries(self._transactional_queries).keys()
        ]
        return Workload(
            period=self._period,
            analytical_queries=analytics,
            transactional_queries=transactions,
            transaction_arrival_count=self._total_transaction_count,
            table_sizes=self._table_sizes,
        )

    def for_period(self, period: timedelta) -> "WorkloadBuilder":
        self._period = period
        return self

    def uniform_per_analytical_query_rate(
        self, count: int, period: Optional[timedelta] = None
    ) -> "WorkloadBuilder":
        """
        Used to express that all queries run `count` times during the period. If
        `period` is None, it defaults to the current period in the workload.
        """
        if period is None:
            self._analytics_count_per = count
        else:
            scaled = count / period.total_seconds() * self._period.total_seconds()
            self._analytics_count_per = int(scaled)
        return self

    def uniform_total_transaction_rate(
        self, count: int, period: Optional[timedelta] = None
    ) -> "WorkloadBuilder":
        """
        Used to express the rate of transactions arriving during the period. If
        `period` is None, it defaults to the current period in the workload.

        Note that this is a global *transaction* rate, not a per-query rate.
        This is because a transaction may consist of multiple queries.
        """
        if period is None:
            self._total_transaction_count = count
        else:
            scaled = count / period.total_seconds() * self._period.total_seconds()
            self._total_transaction_count = int(scaled)
        return self

    def add_analytical_queries_from_file(
        self, file_path: str | pathlib.Path
    ) -> "WorkloadBuilder":
        with open(file_path, encoding="UTF-8") as analytics:
            for q in analytics:
                self._analytical_queries.append(q.strip())
        return self

    def add_transactional_queries_from_file(
        self, file_path: str | pathlib.Path
    ) -> "WorkloadBuilder":
        with open(file_path, encoding="UTF-8") as txns:
            for q in txns:
                self._transactional_queries.append(q.strip())
        return self

    def table_sizes_from_engines(
        self, blueprint: Blueprint, table_sizer: TableSizer
    ) -> "WorkloadBuilder":
        preferred_sources = [Engine.Redshift, Engine.Aurora, Engine.Athena]
        self._table_sizes.clear()
        for table, locations in blueprint.tables_with_locations():
            for source in preferred_sources:
                if source not in locations:
                    continue
                self._table_sizes[table.name] = table_sizer.table_size_rows(
                    table.name, source
                )
                break
            assert table.name in self._table_sizes
        return self

    def _deduplicate_queries(self, queries: List[str]) -> Dict[str, int]:
        """
        Deduplication is by exact string match only.
        """
        deduped: Dict[str, int] = {}
        for q in queries:
            if q in deduped:
                deduped[q] += 1
            else:
                deduped[q] = 1
        return deduped
