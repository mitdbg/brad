import pathlib
import logging
import numpy as np
import numpy.typing as npt
from typing import Dict

from brad.config.engine import Engine
from brad.planner.scoring.performance.analytics_latency import AnalyticsLatencyScorer
from brad.planner.workload import Workload

logger = logging.getLogger(__name__)


class PrecomputedPredictions(AnalyticsLatencyScorer):
    """
    Provides predictions for a fixed workload using precomputed predictions.
    Used for debugging purposes.
    """

    @classmethod
    def load_from_standard_dataset(
        cls, dataset_path: str | pathlib.Path
    ) -> "PrecomputedPredictions":
        if isinstance(dataset_path, pathlib.Path):
            dsp = dataset_path
        else:
            dsp = pathlib.Path(dataset_path)

        with open(dsp / "queries.sql", "r", encoding="UTF-8") as query_file:
            raw_queries = [line.strip() for line in query_file]

        queries_map = {}
        for idx, query in enumerate(raw_queries):
            if query.endswith(";"):
                queries_map[query[:-1]] = idx
            else:
                queries_map[query] = idx

        rt_preds = np.load(dsp / "pred-run_time_s-athena-aurora-redshift.npy")

        # Sanity check.
        assert rt_preds.shape[0] == len(raw_queries)

        preds = [np.array([]), np.array([]), np.array([])]
        preds[Workload.EngineLatencyIndex[Engine.Aurora]] = rt_preds[:, 1]
        preds[Workload.EngineLatencyIndex[Engine.Redshift]] = rt_preds[:, 2]
        preds[Workload.EngineLatencyIndex[Engine.Athena]] = rt_preds[:, 0]

        predictions = np.stack(preds, axis=-1)

        # Replace any `inf` / `nan` values in the predictions with this value.
        # This prevents a degenerate case in performance estimation.
        timeout_value_s = 210.0
        predictions[np.isinf(predictions)] = timeout_value_s
        predictions[np.isnan(predictions)] = timeout_value_s

        return cls(queries_map, predictions)

    @classmethod
    def load(
        cls,
        workload_file_path: str | pathlib.Path,
        aurora_predictions_path: str | pathlib.Path,
        redshift_predictions_path: str | pathlib.Path,
        athena_predictions_path: str | pathlib.Path,
    ) -> "PrecomputedPredictions":
        with open(workload_file_path, "r", encoding="UTF-8") as query_file:
            raw_queries = [line.strip() for line in query_file]

        queries_map = {}
        for idx, query in enumerate(raw_queries):
            if query.endswith(";"):
                queries_map[query[:-1]] = idx
            else:
                queries_map[query] = idx

        aurora = np.load(aurora_predictions_path)
        redshift = np.load(redshift_predictions_path)
        athena = np.load(athena_predictions_path)

        # Sanity check.
        assert aurora.shape[0] == len(raw_queries)
        assert redshift.shape[0] == len(raw_queries)
        assert athena.shape[0] == len(raw_queries)

        preds = [np.array([]), np.array([]), np.array([])]
        preds[Workload.EngineLatencyIndex[Engine.Aurora]] = aurora
        preds[Workload.EngineLatencyIndex[Engine.Redshift]] = redshift
        preds[Workload.EngineLatencyIndex[Engine.Athena]] = athena

        predictions = np.stack(preds, axis=-1)

        # Replace any `inf` values in the predictions with this value. This
        # prevents a degenerate case in performance estimation.
        timeout_value_s = 210.0
        predictions[np.isinf(predictions)] = timeout_value_s

        return cls(queries_map, predictions)

    def __init__(self, queries_map: Dict[str, int], predictions: npt.NDArray) -> None:
        self._queries_map = queries_map
        self._predictions = predictions

    def apply_predicted_latencies(self, workload: Workload) -> None:
        query_indices = []
        has_unmatched = False
        for query in workload.analytical_queries():
            try:
                query_str = query.raw_query.strip()
                if query_str.endswith(";"):
                    query_str = query_str[:-1]
                query_indices.append(self._queries_map[query_str])
            except KeyError:
                logger.warning("Cannot match query:\n%s", query.raw_query.strip())
                query_indices.append(-1)
                has_unmatched = True

        if has_unmatched:
            raise RuntimeError("Workload contains unmatched queries.")
        workload.set_predicted_analytical_latencies(self._predictions[query_indices, :])
