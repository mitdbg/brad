import pathlib
import numpy as np
import numpy.typing as npt
from typing import Dict

from brad.config.engine import Engine
from brad.planner.scoring.performance.analytics_latency import AnalyticsLatencyScorer
from brad.planner.workload import Workload


class PrecomputedPredictions(AnalyticsLatencyScorer):
    """
    Provides predictions for a fixed workload using precomputed predictions.
    Used for debugging purposes.
    """

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

        queries_map = {query: idx for idx, query in enumerate(raw_queries)}
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

        return cls(queries_map, predictions)

    def __init__(self, queries_map: Dict[str, int], predictions: npt.NDArray) -> None:
        self._queries_map = queries_map
        self._predictions = predictions

    def apply_predicted_latencies(self, workload: Workload) -> None:
        query_indices = []
        for query in workload.analytical_queries():
            query_indices.append(self._queries_map[query.raw_query.strip()])
        workload.set_predicted_analytical_latencies(self._predictions[query_indices, :])
