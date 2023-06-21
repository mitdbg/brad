import pathlib

import numpy as np
import numpy.typing as npt

from typing import List
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from imblearn.over_sampling import RandomOverSampler

from . import ORDERED_ENGINES, ENGINE_LABELS
from brad.query_rep import QueryRep
from brad.blueprint.table import Table


class ForestTrainer:
    # Run time NDArray dimensions
    N = 0
    m = 1
    TIMEOUT_VALUE_S = 210

    def __init__(self, schema: List[Table], queries: List[str], run_times: npt.NDArray):
        """
        `queries` is expected to be a list of valid SQL queries.

        `run_times` is expected to be of shape (N, m) where `N` is the number of
        queries and `m` is the number of engines (equal to 3). Use `inf` to
        denote a time out.
        """
        assert len(run_times.shape) == 2
        num_samples, num_engines = run_times.shape
        assert num_engines == len(ORDERED_ENGINES)
        assert num_samples == len(queries)

        self._schema = schema

        # Raw data.
        self._raw_queries = queries
        self._raw_run_times = run_times

        self._preprocess_training_data()
        self._compute_features()

    def _preprocess_training_data(self) -> None:
        # Used to remove queries that time out everywhere.
        all_timeout_idx = np.where(np.all(np.isinf(self._raw_run_times), axis=self.m))

        # Create a (N, m) validity mask.
        val_mask = np.ones(self._raw_run_times.shape[self.N], dtype=bool)
        val_mask[all_timeout_idx] = False
        val_mask = np.expand_dims(val_mask, axis=self.m)
        val_mask = np.concatenate([val_mask] * len(ORDERED_ENGINES), axis=self.m)

        # Filter out queries that time out across all engines.
        valid_queries = list(
            map(
                lambda tup: QueryRep(tup[1]),
                filter(
                    lambda tup: tup[0] not in all_timeout_idx,
                    enumerate(self._raw_queries),
                ),
            )
        )

        # Replace `inf` values with the time out value and remove queries that
        # time out across all engines.
        run_times = self._raw_run_times.copy()
        run_times[np.isinf(run_times)] = self.TIMEOUT_VALUE_S
        run_times = run_times[val_mask]

        # Pre-compute:
        # - Oracle based routing decisions
        # - Best run time when using the oracle's decisions
        oracle_routing = np.argmin(run_times, axis=self.m)
        oracle_times = np.amin(run_times, axis=self.m)

        self._valid_queries = valid_queries
        self._run_times = run_times
        self._oracle_routing = oracle_routing
        self._oracle_times = oracle_times

    def _compute_features(self) -> None:
        # Compute query features.
        table_order = [table.name for table in self._schema]
        f_table_presence = []
        for q in self._valid_queries:
            features = np.zeros(len(table_order))
            for t in q.tables():
                try:
                    tbl_idx = table_order.index(t)
                    features[tbl_idx] = 1
                except ValueError:
                    pass
            f_table_presence.append(features)

        self._table_order = table_order
        self._f_table_presence = np.array(f_table_presence)

    def train(self):
        pass
