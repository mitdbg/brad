import pickle
import numpy as np
import numpy.typing as npt

from datetime import timedelta
from typing import Dict, List, Tuple, Optional, Iterable
from itertools import chain
from pathlib import Path
from itertools import combinations

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.planner.workload.query import Query
from brad.utils.table_sizer import TableSizer


class Workload:
    """
    A representation of the workload to be considered during the blueprint
    planning process. Each workload represents a fixed period of time.

    The main properties on a workload are

    - The period length (the period of time represented by this workload)
    - Analytics:
      - The raw SQL of each query
      - Each query's arrival count in the epoch (e.g., 2 means
        twice during the period)
    - Transactions
      - List of the queries that appear in transactions
      - Transaction arrival count in the epoch (e.g., 1e6 means 1 million
        transactions arrive during the period). This count is independent of the
        individual queries; we only look at the transaction rate in aggregate.
    - Dataset statistics
      - Logical table sizes (number of rows per table)

    The planner uses the `Workload` when computing scores to compare blueprints.
    The intention is that these values can also be _forecasted_.
    """

    # Used to extract predicted latency (dimension index).
    EngineLatencyIndex = {
        Engine.Aurora: 0,
        Engine.Redshift: 1,
        Engine.Athena: 2,
    }

    @classmethod
    def empty(cls) -> "Workload":
        return cls(timedelta(hours=1), [], [], 0, {})

    @classmethod
    def from_pickle(cls, file_path: str | Path) -> "Workload":
        with open(file_path, "rb") as in_file:
            return pickle.load(in_file)

    def __init__(
        self,
        period: timedelta,
        analytical_queries: List[Query],
        transactional_queries: List[Query],
        transaction_arrival_count: int,
        table_sizes: Dict[str, int],
    ) -> None:
        self._period = period
        self._analytical_queries: List[Query] = analytical_queries
        self._transactional_queries: List[Query] = transactional_queries
        self._transaction_arrival_count = transaction_arrival_count
        self._table_sizes = table_sizes

        # The predicted latencies of the analytical queries.
        # This property is set and used by the blueprint planner.
        #
        # Shape: (N x 3) where `N` is the number of queries and 3 represents our
        # three engines (Aurora, Redshift, Athena) in that order.
        self._predicted_analytical_latencies: Optional[npt.NDArray] = None

        ###
        ### Legacy properties below.
        ###

        # The size of a table on an engine.
        self._aurora_row_size_bytes: Dict[str, int] = {}
        self._table_sizes_mb: Dict[Tuple[str, Engine], int] = {}
        self._dataset_size_mb = 0

    def serialize_for_debugging(self, output_path: str | Path) -> None:
        with open(output_path, "wb") as out_file:
            pickle.dump(self, out_file)

    def period(self) -> timedelta:
        return self._period

    def analytical_queries(self) -> List[Query]:
        return self._analytical_queries

    def transactional_queries(self) -> List[Query]:
        return self._transactional_queries

    def all_queries(self) -> Iterable[Query]:
        return chain(self._transactional_queries, self._analytical_queries)

    def transaction_arrival_count(self) -> int:
        return self._transaction_arrival_count

    ###
    ### The methods below are meant for the blueprint planner.
    ###

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

    ###
    ### The methods below are legacy code.
    ###

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
