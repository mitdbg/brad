from typing import Dict, List, Tuple, Optional, Iterable
from itertools import chain
from pathlib import Path
from itertools import combinations
import boto3
import re
import numpy as np
import numpy.typing as npt

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.planner.workload.query import Query
from brad.utils.table_sizer import TableSizer
from brad.config.file import ConfigFile


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

    # Used to extract predicted latency (dimension index).
    EngineLatencyIndex = {
        Engine.Aurora: 0,
        Engine.Redshift: 1,
        Engine.Athena: 2,
    }

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

    @classmethod
    def from_s3_logs(cls, config: ConfigFile, epochs: int):
        s3 = boto3.client("s3")

        # List all objects in the directory
        response = s3.list_objects_v2(
            Bucket=config.s3_logs_bucket, Prefix=config.s3_logs_path
        )

        # Get the last `epochs` epochs by sorting the objects based on their key names
        # TODO: think about scenarios where files haven't been uploaded yet etc. - are the last k on S3 the most recent k?
        sorted_files = sorted(
            response["Contents"], key=lambda obj: obj["Key"], reverse=True
        )[: epochs * 2]

        txn_queries = []
        analytical_queries = []
        sampling_prob = 1

        # Retrieve the contents of each file
        for file_obj in sorted_files:
            file_key = file_obj["Key"]

            response = s3.get_object(Bucket=config.s3_logs_bucket, Key=file_key)
            content = response["Body"].read().decode("utf-8")

            if "analytical" in file_key:
                for line in content.strip().split("\n"):
                    q = re.findall(r"Query: (.+?) Engine:", line)[0]
                    analytical_queries.append(Query(q))
            elif "transactional" in file_key:
                prob = re.findall(r"_p(\d+)\.log$", file_key)[0]
                if (float(prob) / 100.0) < sampling_prob:
                    sampling_prob = prob
                for line in content.strip().split("\n"):
                    print(line)
                    q = re.findall(r"Query: (.+) Engine:", line)[0]
                    txn_queries.append(Query(q))

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

        # The predicted latencies of the analytical queries.
        # Shape: (N x 3) where `N` is the number of queries and 3 represents our
        # three engines (Aurora, Redshift, Athena) in that order.
        self._predicted_analytical_latencies: Optional[npt.NDArray] = None

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

    def populate_table_sizes_using_blueprint(
        self, blueprint: Blueprint, table_sizer: TableSizer
    ) -> None:
        self._table_sizes_mb.clear()
        for table, locations in blueprint.tables_with_locations():
            for loc in locations:
                self._table_sizes_mb[(table.name, loc)] = table_sizer.table_size_mb(
                    table.name, loc
                )

            # Fetch the row size as well, if applicable.
            if Engine.Aurora in locations:
                self._aurora_row_size_bytes[
                    table.name
                ] = table_sizer.aurora_row_size_bytes(table.name)

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

    def set_predicted_analytical_latencies(
        self, predicted_latency: npt.NDArray
    ) -> None:
        self._predicted_analytical_latencies = predicted_latency

    def get_predicted_analytical_latency(self, query_idx: int, engine: Engine) -> float:
        assert self._predicted_analytical_latencies is not None
        return self._predicted_analytical_latencies[
            query_idx, self.EngineLatencyIndex[engine]
        ].item()

    def get_predicted_analytical_latency_batch(
        self, query_indices: List[int], engine: Engine
    ) -> npt.NDArray:
        assert self._predicted_analytical_latencies is not None
        return self._predicted_analytical_latencies[
            query_indices, self.EngineLatencyIndex[engine]
        ]

    def compute_latency_gains(self) -> npt.NDArray:
        """
        We define "gain" as the largest ratio between predicted execution times
        across engines. The intuition is that a high gain represents a query
        where routing correctly will have a large impact on its latency.
        """
        preds = self._predicted_analytical_latencies
        assert preds is not None
        num_engines = preds.shape[1]
        ratios = []
        for i, j in combinations(range(num_engines), 2):
            ratios.append(preds[:, i] / preds[:, j])
            ratios.append(preds[:, j] / preds[:, i])
        combined = np.stack(ratios, axis=1)
        gains = np.amax(combined, axis=1)
        return gains
