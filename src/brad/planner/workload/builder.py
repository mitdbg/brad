import csv
import pathlib
import logging
import re
from datetime import timedelta, datetime
from typing import List, Dict, Optional

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.planner.workload import Workload
from brad.planner.workload.query import Query
from brad.utils.table_sizer import TableSizer
from brad.workload_logging.log_fetcher import LogFetcher

logger = logging.getLogger(__name__)


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
        self._period = timedelta(hours=1)
        self._table_sizes: Dict[str, int] = {}
        self._prespecified_queries: List[Query] = []

    def build(
        self,
        rescale_to_period: Optional[timedelta] = None,
        reinterpret_second_as: Optional[timedelta] = None,
    ) -> Workload:
        """
        Change the workload's period using `rescale_period`. We linearly scale
        the query counts.
        """
        if reinterpret_second_as is not None:
            # This is used to "scale up" the workload (in time) without actually
            # having to wait for the workload to complete.
            logger.info(
                "NOTICE: Constructing workload where 1 second is interpreted as %d seconds.",
                reinterpret_second_as.total_seconds(),
            )
            self._period = timedelta(
                seconds=self._period.total_seconds()
                * reinterpret_second_as.total_seconds()
            )

        if rescale_to_period is None or self._period.total_seconds() == 0.0:
            multiplier = 1.0
        else:
            multiplier = rescale_to_period / self._period

        if len(self._analytical_queries) > 0:
            analytics = [
                Query(
                    q,
                    arrival_count=max(
                        int(self._analytics_count_per * count * multiplier), 1
                    ),
                )
                for q, count in self._deduplicate_queries(
                    self._analytical_queries
                ).items()
            ]
        else:
            if rescale_to_period is not None:
                analytics = [
                    Query(q.raw_query, max(int(q.arrival_count() * multiplier), 1))
                    for q in self._prespecified_queries
                ]
            else:
                analytics = self._prespecified_queries

        transactions = [
            # N.B. `count` is sampled!
            Query(q, arrival_count=max(int(count * multiplier), 1))
            for q, count in self._deduplicate_queries(
                self._transactional_queries
            ).items()
        ]

        return Workload(
            period=self._period if rescale_to_period is None else rescale_to_period,
            analytical_queries=analytics,
            transactional_queries=transactions,
            table_sizes=self._table_sizes,
        )

    def for_period(self, period: timedelta) -> "WorkloadBuilder":
        self._period = period
        return self

    def uniform_per_analytical_query_rate(
        self, count: float, period: Optional[timedelta] = None
    ) -> "WorkloadBuilder":
        """
        Used to express that all queries run `count` times during the period. If
        `period` is None, it defaults to the current period in the workload.
        """
        if period is None:
            self._analytics_count_per = int(count)
        else:
            scaled = count / period.total_seconds() * self._period.total_seconds()
            self._analytics_count_per = int(scaled)
        if self._analytics_count_per == 0:
            logger.warning(
                "Analytical rate rounded down to 0 queries. Original count: %f", count
            )
        return self

    def add_analytical_queries_and_counts_from_file(
        self,
        query_bank_file: str | pathlib.Path,
        query_counts: str | pathlib.Path,
        multiplier: int = 1,
    ) -> "WorkloadBuilder":
        self._analytical_queries.clear()

        with open(query_bank_file, encoding="UTF-8") as bank:
            all_queries = [q.strip() for q in bank]

        with open(query_counts, encoding="UTF-8") as queries:
            reader = csv.reader(queries)
            for i, row in enumerate(reader):
                if i == 0:
                    continue
                query_idx = int(row[0])
                run_count = int(row[1])
                self._prespecified_queries.append(
                    Query(all_queries[query_idx], run_count * multiplier)
                )
        return self

    def add_analytical_queries_from_file(
        self, file_path: str | pathlib.Path
    ) -> "WorkloadBuilder":
        self._prespecified_queries.clear()

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

    def add_queries_from_s3_logs(
        self, config: ConfigFile, window_start: datetime, window_end: datetime
    ) -> "WorkloadBuilder":
        assert window_start <= window_end
        self._prespecified_queries.clear()

        log_fetcher = LogFetcher(config)

        txn_queries = []
        analytical_queries = []
        sampling_prob = 1  # Currently unused.

        range_end: Optional[datetime] = None
        range_start: Optional[datetime] = None
        epoch_length = config.epoch_length

        # The logic below extracts data from log files that represent epochs
        # that intersect with the provided window.
        #
        # NOTE: This logic will overcount the time period if there are log gaps
        # in the window (e.g., the window spans multiple epochs and we did not
        # log a few epochs in the middle of the window). This behavior is OK for
        # our use cases since we will assume that the query logger runs
        # continuously.

        for log_file in log_fetcher.fetch_logs(
            window_start, window_end, include_contents=True
        ):
            is_valid = False

            if "analytical" in log_file.file_key:
                for line in log_file.contents.strip().split("\n"):
                    matches = re.findall(r"Query: (.+?) Engine:", line)
                    if len(matches) == 0:
                        continue
                    q = matches[0]
                    analytical_queries.append(q.strip())
                is_valid = True

            elif "transactional" in log_file.file_key:
                prob = re.findall(r"_p(\d+)\.log$", log_file.file_key)[0]
                prob = float(prob) / 100.0
                if (prob / 100.0) < sampling_prob:
                    sampling_prob = prob
                for line in log_file.contents.strip().split("\n"):
                    matches = re.findall(r"Query: (.+) Engine:", line)
                    if len(matches) == 0:
                        continue
                    q = matches[0]
                    txn_queries.append(q.strip())
                is_valid = True

            # Adjust the processed range of data.
            if is_valid:
                if range_start is None:
                    range_start = log_file.epoch_start
                else:
                    range_start = min(range_start, log_file.epoch_start)

                if range_end is None:
                    range_end = log_file.epoch_start + epoch_length
                else:
                    range_end = max(range_end, log_file.epoch_start + epoch_length)

        # Sanity checks.
        if range_start is None or range_end is None:
            # No queries match.
            self._period = timedelta(seconds=0)
            return self

        self._transactional_queries.extend(txn_queries)
        self._analytical_queries.extend(analytical_queries)
        self._period = range_end - range_start

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
