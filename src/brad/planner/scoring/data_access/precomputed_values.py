import logging
import pathlib
import numpy as np
import numpy.typing as npt
from typing import Dict, Tuple, List

from .provider import DataAccessProvider
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

        queries_map = {query: idx for idx, query in enumerate(raw_queries)}
        queries_map = {}
        for idx, query in enumerate(raw_queries):
            if query.endswith(";"):
                queries_map[query[:-1]] = idx
            else:
                queries_map[query] = idx

        data_stats = np.load(dsp / "pred-data_accessed-athena-aurora.npy")
        # TODO: Maybe we might want a better placeholder.
        data_stats[np.isnan(data_stats)] = 0

        aurora = data_stats[:, 1]
        athena = data_stats[:, 0]
        assert len(aurora.shape) == 1
        assert len(athena.shape) == 1

        return cls(name, queries_map, aurora, athena)

    def __init__(
        self,
        name: str,
        queries_map: Dict[str, int],
        aurora_accessed_pages: npt.NDArray,
        athena_accessed_bytes: npt.NDArray,
    ) -> None:
        self.name = name
        self.queries_map = queries_map
        self.aurora_accessed_pages = aurora_accessed_pages
        self.athena_accessed_bytes = athena_accessed_bytes

    def extract_access_statistics(
        self, workload: Workload
    ) -> Tuple[List[int], List[int], npt.NDArray, npt.NDArray]:
        workload_query_index = []
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
            self.aurora_accessed_pages[indices_in_dataset],
            self.athena_accessed_bytes[indices_in_dataset],
        )


class PrecomputedDataAccessProvider(DataAccessProvider):
    """
    Provides predictions for a fixed workload using precomputed values.
    Used for debugging purposes.
    """

    @classmethod
    def load_from_standard_dataset(
        cls,
        datasets: List[Tuple[str, str | pathlib.Path]],
    ) -> "PrecomputedDataAccessProvider":
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
        aurora_accessed_pages_path: str | pathlib.Path,
        athena_accessed_bytes_path: str | pathlib.Path,
    ):
        with open(workload_file_path, "r", encoding="UTF-8") as query_file:
            raw_queries = [line.strip() for line in query_file]

        queries_map = {query: idx for idx, query in enumerate(raw_queries)}
        queries_map = {}
        for idx, query in enumerate(raw_queries):
            if query.endswith(";"):
                queries_map[query[:-1]] = idx
            else:
                queries_map[query] = idx

        aurora = np.load(aurora_accessed_pages_path)
        athena = np.load(athena_accessed_bytes_path)
        assert len(aurora.shape) == 1
        assert len(athena.shape) == 1
        assert aurora.shape[0] == athena.shape[0]

        return cls([QueryMap("custom", queries_map, aurora, athena)])

    def __init__(
        self,
        predictions: List[QueryMap],
    ) -> None:
        self._predictions = predictions

    def apply_access_statistics(self, workload: Workload) -> None:
        all_queries = workload.analytical_queries()
        applied_aurora = np.ones(len(all_queries)) * np.nan
        applied_athena = np.ones(len(all_queries)) * np.nan

        for qm in self._predictions:
            workload_indices, _, aurora, athena = qm.extract_access_statistics(workload)
            applied_aurora[workload_indices] = aurora
            applied_athena[workload_indices] = athena

        # Special case: vector similarity queries.
        special_vector_queries = []
        for wqi, q in enumerate(all_queries):
            if "<=>" in q.raw_query:
                special_vector_queries.append(wqi)
        applied_athena[special_vector_queries] = 0.0
        applied_aurora[special_vector_queries] = 0.0

        # Check for unmatched queries.
        num_unmatched_athena = np.isnan(applied_athena).sum()
        if num_unmatched_athena > 0:
            raise RuntimeError("Unmatched Athena queries: " + num_unmatched_athena)
        num_unmatched_aurora = np.isnan(applied_aurora).sum()
        if num_unmatched_aurora > 0:
            raise RuntimeError("Unmatched Aurora queries: " + num_unmatched_aurora)

        workload.set_predicted_data_access_statistics(
            aurora_pages=applied_aurora,
            athena_bytes=applied_athena,
        )
