import boto3
import csv
import pathlib
import logging
import re
from datetime import timedelta, datetime, timezone
from typing import List, Dict, Optional

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.planner.workload import Workload
from brad.planner.workload.query import Query
from brad.utils.table_sizer import TableSizer

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

    def build(self, rescale_to_period: Optional[timedelta] = None) -> Workload:
        """
        Change the workload's period using `rescale_period`. We linearly scale
        the query counts.
        """
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

        s3 = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key,
            aws_secret_access_key=config.aws_access_key_secret,
        )

        response = s3.list_objects_v2(
            Bucket=config.s3_logs_bucket, Prefix=config.s3_logs_path
        )

        # TODO: This may miss logs that are in the process of being uploaded.
        sorted_files = sorted(
            response["Contents"], key=lambda obj: obj["Key"], reverse=True
        )
        file_idx = 0

        txn_queries = []
        analytical_queries = []
        sampling_prob = 1  # Currently unused.

        range_end: Optional[datetime] = None
        range_start: Optional[datetime] = None

        def extract_epoch_start(file_stem: str) -> datetime:
            return datetime.strptime(
                " ".join(file_stem.split("_")[:2]), "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=timezone.utc)

        def intervals_intersect(
            start1: datetime, end1: datetime, start2: datetime, end2: datetime
        ) -> bool:
            # Left endpoint is inclusive, right endpoint is exclusive.
            return not (end1 <= start2 or end2 <= start1)

        epoch_length = config.epoch_length

        # The logic below extracts data from log files that represent epochs
        # that intersect with the provided window.
        #
        # NOTE: This logic will overcount the time period if there are log gaps
        # in the window (e.g., the window spans multiple epochs and we did not
        # log a few epochs in the middle of the window). This behavior is OK for
        # our use cases since we will assume that the query logger runs
        # continuously.

        # Skip files that represent epochs after `window_end`.
        while file_idx < len(sorted_files):
            file_obj = sorted_files[file_idx]
            file_key = file_obj["Key"]
            file_stem = file_key.split("/")[-1]

            epoch_start = extract_epoch_start(file_stem)
            epoch_end = epoch_start + epoch_length

            if not intervals_intersect(
                epoch_start, epoch_end, window_start, window_end
            ):
                file_idx += 1
            else:
                # This assumes the epoch length did not change from when the
                # data was logged to now.
                range_end = epoch_end
                range_start = epoch_start
                break

        # Retrieve the contents of each overlapping file.
        while file_idx < len(sorted_files):
            file_obj = sorted_files[file_idx]
            file_key = file_obj["Key"]
            file_stem = file_key.split("/")[-1]

            epoch_start = extract_epoch_start(file_stem)
            epoch_end = epoch_start + epoch_length
            if not intervals_intersect(
                epoch_start, epoch_end, window_start, window_end
            ):
                break

            logger.debug("Processing log at epoch start: %s", epoch_start)

            response = s3.get_object(Bucket=config.s3_logs_bucket, Key=file_key)
            content = response["Body"].read().decode("utf-8")

            if "analytical" in file_key:
                for line in content.strip().split("\n"):
                    matches = re.findall(r"Query: (.+?) Engine:", line)
                    if len(matches) == 0:
                        continue
                    q = matches[0]
                    analytical_queries.append(q.strip())

            elif "transactional" in file_key:
                prob = re.findall(r"_p(\d+)\.log$", file_key)[0]
                prob = float(prob) / 100.0
                if (prob / 100.0) < sampling_prob:
                    sampling_prob = prob
                for line in content.strip().split("\n"):
                    matches = re.findall(r"Query: (.+) Engine:", line)
                    if len(matches) == 0:
                        continue
                    q = matches[0]
                    txn_queries.append(q.strip())

            range_start = epoch_start
            file_idx += 1

        # Sanity checks.
        if range_end is None:
            assert range_start is None
            # No queries match.
            self._period = timedelta(seconds=0)
            return self

        # If `range_end` is defined, we must have executed the second loop at
        # least once.
        assert range_start is not None

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
