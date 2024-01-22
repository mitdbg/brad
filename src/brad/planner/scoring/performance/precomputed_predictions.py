import pathlib
import logging
import numpy as np
import numpy.typing as npt
from typing import Dict, List, Tuple

from brad.config.engine import Engine
from brad.planner.scoring.performance.analytics_latency import AnalyticsLatencyScorer
from brad.planner.workload import Workload

logger = logging.getLogger(__name__)


class QueryMap:
    @classmethod
    def load_from_standard_dataset(
        cls,
        name: str,
        dataset_path: str | pathlib.Path,
    ) -> "QueryMap":
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

        # Check for any bad data (all predictions are NaN or Inf).
        all_inf = np.all(np.isinf(predictions), axis=1)
        all_nan = np.all(np.isnan(predictions), axis=1)

        # Replace any `inf` / `nan` values in the predictions with this value.
        # This prevents a degenerate case in performance estimation.
        timeout_value_s = 210.0
        predictions[np.isinf(predictions)] = timeout_value_s
        predictions[np.isnan(predictions)] = timeout_value_s

        if np.any(all_inf):
            all_inf_idxs = np.where(all_inf)[0]
            logger.warning(
                "[Dataset %s] Some predicted data points are inf on all engines. "
                "They will be reset to 0.0. %s",
                name,
                str(all_inf_idxs),
            )
            predictions[all_inf_idxs, [0, 1, 2]] = 0.0

        if np.any(all_nan):
            all_nan_idxs = np.where(all_nan)[0]
            logger.warning(
                "[Dataset %s] Some predicted data points are inf on all engines. "
                "They will be reset to 0.0. %s",
                name,
                str(all_nan_idxs),
            )
            predictions[all_nan_idxs, [0, 1, 2]] = 0.0

        negative_value_mask = predictions < 0.0
        if np.any(negative_value_mask):
            negative_idxs = np.any(negative_value_mask, axis=1)
            logger.warning(
                "[Dataset %s] Some predicted points are negative. "
                "Will be reset to 0.01. %s",
                name,
                str(np.where(negative_idxs)[0]),
            )
            # Imputed value to avoid degenerate cases.
            predictions[negative_value_mask] = 0.01

        return cls(name, queries_map, predictions)

    def __init__(
        self, name: str, queries_map: Dict[str, int], predictions: npt.NDArray
    ) -> None:
        self.name = name
        self.queries_map = queries_map
        self.predictions = predictions

    def extract_matched_predictions(
        self, workload: Workload
    ) -> Tuple[List[int], List[int], npt.NDArray]:
        """
        Returns the matched indices and the predicted values.
        """
        # The index of the query in the input workload.
        workload_query_index = []
        # The index of the query in the precomputed predictions bank.
        indices_in_dataset = []
        for wqi, query in enumerate(workload.analytical_queries()):
            try:
                query_str = query.raw_query.strip()
                if query_str.endswith(";"):
                    query_str = query_str[:-1]
                indices_in_dataset.append(self.queries_map[query_str])
                workload_query_index.append(wqi)
            except KeyError:
                continue
        return (
            workload_query_index,
            indices_in_dataset,
            self.predictions[indices_in_dataset, :],
        )


class PrecomputedPredictions(AnalyticsLatencyScorer):
    """
    Provides predictions for a fixed workload using precomputed predictions.
    Used for debugging purposes.
    """

    @classmethod
    def load_from_standard_dataset(
        cls,
        datasets: List[Tuple[str, str | pathlib.Path]],
    ) -> "PrecomputedPredictions":
        return cls(
            [
                QueryMap.load_from_standard_dataset(name, dataset_path)
                for name, dataset_path in datasets
            ]
        )

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

        logger.warning(
            "Running older precomputed predictions codepath. "
            "Note that this does not correct for data errors."
        )

        return cls([QueryMap("custom", queries_map, predictions)])

    def __init__(self, predictions: List[QueryMap]) -> None:
        self._predictions = predictions

    def apply_predicted_latencies(self, workload: Workload) -> None:
        all_queries = workload.analytical_queries()
        applied_predictions = np.ones((len(all_queries), 3)) * np.nan
        debug_map = {}

        for qm in self._predictions:
            workload_indices, dataset_indices, preds = qm.extract_matched_predictions(
                workload
            )
            applied_predictions[workload_indices] = preds
            debug_map[qm.name] = list(zip(workload_indices, dataset_indices))

        # Special case: vector similarity queries.
        special_vector_queries = []
        for wqi, q in enumerate(all_queries):
            if "<=>" in q.raw_query:
                special_vector_queries.append(wqi)

        applied_predictions[
            special_vector_queries, Workload.EngineLatencyIndex[Engine.Aurora]
        ] = 3.6
        debug_map["special_vector"] = list(
            zip(special_vector_queries, [-1] * len(special_vector_queries))
        )

        # Check for unmatched queries.
        num_unmatched_aurora = np.isnan(
            applied_predictions[:, Workload.EngineLatencyIndex[Engine.Aurora]]
        ).sum()
        if num_unmatched_aurora > 0:
            raise RuntimeError(f"Unmatched queries: {num_unmatched_aurora.item()}")

        workload.set_predicted_analytical_latencies(applied_predictions, debug_map)
