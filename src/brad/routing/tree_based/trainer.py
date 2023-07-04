import pathlib

import numpy as np
import numpy.typing as npt

from typing import List, Dict, Tuple, Optional
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from imblearn.over_sampling import RandomOverSampler

from . import ORDERED_ENGINES
from .model_wrap import ModelWrap
from brad.blueprint.table import Table
from brad.blueprint.user import UserProvidedBlueprint
from brad.data_stats.estimator import Estimator
from brad.query_rep import QueryRep
from brad.routing.policy import RoutingPolicy


ModelQuality = Dict[str, Dict[str, float]]


class ForestTrainer:
    # Run time NDArray dimensions
    N = 0
    m = 1
    TIMEOUT_VALUE_S = 210

    def __init__(
        self,
        policy: RoutingPolicy,
        schema: List[Table],
        queries: List[str],
        run_times: npt.NDArray,
    ):
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

        self._policy = policy
        self._schema = schema

        # Raw data.
        self._raw_queries = queries
        self._raw_run_times = run_times

        # Features.
        self._table_order: List[str] = []
        self._f_table_presence = np.array([])
        self._f_table_selectivity = np.array([])

        self._preprocess_training_data()

    @classmethod
    def load_saved_data(
        cls,
        policy: RoutingPolicy,
        schema_file: str | pathlib.Path,
        queries_file: str | pathlib.Path,
        aurora_run_times: str | pathlib.Path,
        redshift_run_times: str | pathlib.Path,
        athena_run_times: str | pathlib.Path,
    ) -> "ForestTrainer":
        bp = UserProvidedBlueprint.load_from_yaml_file(schema_file)

        with open(queries_file, "r", encoding="UTF-8") as file:
            raw_queries = [line.strip() for line in file]

        aurora_rt = np.load(aurora_run_times)
        redshift_rt = np.load(redshift_run_times)
        athena_rt = np.load(athena_run_times)

        stacked = np.stack([aurora_rt, redshift_rt, athena_rt], axis=cls.m)

        return cls(policy, bp.tables, raw_queries, stacked)

    def train(
        self,
        train_full: bool = True,
        max_depth: int = 15,
        min_samples_split: int = 10,
        num_trees: int = 100,
    ) -> Tuple[ModelWrap, ModelQuality]:
        # TODO: We should select the hyperparameters above automatically.
        # `sklearn` has helper functions for this, but we have a slightly more
        # involved training pipeline (with resampling), so we are punting on it
        # for now.

        # Run a train/test split at first to get a rough estimate of the
        # forest's quality.
        data = self._split_dataset()
        X, y, qidx = self._resample_data(
            data["inp_train"], data["labels_train"], data["qidx_train"]
        )
        clf = RandomForestClassifier(
            n_estimators=num_trees,
            criterion="entropy",
            max_depth=max_depth,
            min_samples_split=min_samples_split,
        )
        model_for_eval = clf.fit(X, y)
        train_pred = model_for_eval.predict(X)
        test_pred = model_for_eval.predict(data["inp_test"])

        train_quality = self._compute_routing_quality(train_pred, qidx)
        test_quality = self._compute_routing_quality(test_pred, data["qidx_test"])
        quality = {"train": train_quality, "test": test_quality}

        if not train_full:
            return model_for_eval, quality

        X, y, qidx = self._resample_data(
            self._f_table_presence,
            self._oracle_routing,
            np.array(range(len(self._valid_queries))),
        )
        model = clf.fit(X, y)
        train_pred = model.predict(X)
        train_quality = self._compute_routing_quality(train_pred, qidx)

        return ModelWrap(self._policy, self._table_order, model), {
            "train": train_quality,
            # This is an estimated test quality (since it is based on the model
            # trained with held out data).
            "test": test_quality,
        }

    def _preprocess_training_data(self) -> None:
        # Used to remove queries that time out everywhere.
        all_timeout_idx = np.where(np.all(np.isinf(self._raw_run_times), axis=self.m))

        # Create a (N, m) validity mask.
        val_mask = np.ones(self._raw_run_times.shape[self.N], dtype=bool)
        val_mask[all_timeout_idx] = False

        # Filter out queries that time out across all engines.
        valid_queries = list(
            map(
                lambda tup: QueryRep(tup[1]),
                filter(
                    lambda tup: tup[0] not in all_timeout_idx[0],
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

    def compute_features(self, estimator: Optional[Estimator] = None) -> None:
        """
        An estimator must be provided for table selectivity features.
        """
        self._table_order = [table.name for table in self._schema]
        if self._policy == RoutingPolicy.ForestTablePresence:
            self._compute_presence_features()
        elif self._policy == RoutingPolicy.ForestTableSelectivity:
            assert estimator is not None
            self._compute_selectivity_features(estimator)
        else:
            raise RuntimeError("Unsupported routing policy: {}".format(self._policy))

    def _compute_selectivity_features(self, estimator: Estimator) -> None:
        f_table_selectivity = []
        for q in self._valid_queries:
            features = np.zeros(len(self._table_order))
            access_infos = estimator.get_access_info_sync(q)

            for ai in access_infos:
                tidx = self._table_order.index(ai.table_name)
                features[tidx] = max(features[tidx], ai.selectivity)

            f_table_selectivity.append(features)

        self._f_table_selectivity = np.array(f_table_selectivity)

    def _compute_presence_features(self) -> None:
        f_table_presence = []
        for q in self._valid_queries:
            features = np.zeros(len(self._table_order))
            for t in q.tables():
                try:
                    tbl_idx = self._table_order.index(t)
                    features[tbl_idx] = 1
                except ValueError:
                    pass
            f_table_presence.append(features)

        self._f_table_presence = np.array(f_table_presence)

    def _compute_routing_quality(
        self, predictions: npt.NDArray, query_indices: npt.NDArray
    ) -> Dict[str, float]:
        num_queries = predictions.shape[0]

        if query_indices is None:
            query_indices = np.array(range(len(self._valid_queries)))

        # Routing accuracy
        oracle_locations = self._oracle_routing[query_indices]
        accuracy = np.sum(predictions == oracle_locations) / num_queries

        # Workload completion time
        oracle_times = self._oracle_times[query_indices]
        oracle_completion = oracle_times.sum()
        routing_times = self._run_times[query_indices, predictions]
        routed_completion = routing_times.sum()

        # Slowdown over best times
        slowdowns = routing_times / oracle_times

        return {
            "accuracy": accuracy,
            "total_slowdown_rel_oracle": routed_completion / oracle_completion,
            "geomean_slowdown": np.exp(np.log(slowdowns).mean()),
        }

    def _split_dataset(
        self, test_frac: float = 0.2, random_state: int = 0
    ) -> Dict[str, npt.NDArray]:
        if self._policy == RoutingPolicy.ForestTablePresence:
            features = self._f_table_presence
        elif self._policy == RoutingPolicy.ForestTableSelectivity:
            features = self._f_table_selectivity
        else:
            assert False

        (
            X_train,
            X_test,
            y_train,
            y_test,
            qidx_train,
            qidx_test,
        ) = train_test_split(
            features,
            self._oracle_routing,  # Labels
            np.array(range(len(self._valid_queries))),  # Queries in the dataset
            stratify=self._oracle_routing,
            test_size=test_frac,
            random_state=random_state,
        )

        return {
            "inp_train": X_train,
            "inp_test": X_test,
            "labels_train": y_train,
            "labels_test": y_test,
            "qidx_train": qidx_train,
            "qidx_test": qidx_test,
        }

    def _resample_data(
        self, X: npt.NDArray, y: npt.NDArray, query_indices: npt.NDArray
    ) -> Tuple[npt.NDArray, npt.NDArray, npt.NDArray]:
        ros = RandomOverSampler(random_state=0)
        X_res, y_res = ros.fit_resample(X, y)

        ros = RandomOverSampler(random_state=0)
        indices = np.expand_dims(query_indices, axis=1)
        qidx_res, y_res2 = ros.fit_resample(indices, y)
        qidx_res = np.squeeze(qidx_res)

        assert np.all(y_res2 == y_res)

        return X_res, y_res, qidx_res
