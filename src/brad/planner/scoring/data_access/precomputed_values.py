import logging
import pathlib
import numpy as np
import numpy.typing as npt
from typing import Dict

from .provider import DataAccessProvider
from brad.planner.workload import Workload

logger = logging.getLogger(__name__)


class PrecomputedDataAccessProvider(DataAccessProvider):
    """
    Provides predictions for a fixed workload using precomputed values.
    Used for debugging purposes.
    """

    @classmethod
    def load_from_standard_dataset(
        cls,
        dataset_path: str | pathlib.Path,
    ):
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

        data_stats = np.load("pred-data_accessed-athena-aurora.npy")
        # TODO: Maybe we might want a better placeholder.
        data_stats[np.isnan(data_stats)] = 0

        aurora = data_stats[:, 1]
        athena = data_stats[:, 0]
        assert len(aurora.shape) == 1
        assert len(athena.shape) == 1

        return cls(queries_map, aurora, athena)

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

        return cls(queries_map, aurora, athena)

    def __init__(
        self,
        queries_map: Dict[str, int],
        aurora_accessed_pages: npt.NDArray,
        athena_accessed_bytes: npt.NDArray,
    ) -> None:
        self._queries_map = queries_map
        self._aurora_accessed_pages = aurora_accessed_pages
        self._athena_accessed_bytes = athena_accessed_bytes

    def apply_access_statistics(self, workload: Workload) -> None:
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
        workload.set_predicted_data_access_statistics(
            aurora_pages=self._aurora_accessed_pages[query_indices],
            athena_bytes=self._athena_accessed_bytes[query_indices],
        )
