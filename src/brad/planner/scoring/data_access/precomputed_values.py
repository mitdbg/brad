import pathlib
import numpy as np
import numpy.typing as npt
from typing import Dict

from .provider import DataAccessProvider
from brad.planner.workload import Workload


class PrecomputedDataAccessProvider(DataAccessProvider):
    """
    Provides predictions for a fixed workload using precomputed values.
    Used for debugging purposes.
    """

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
        for query in workload.analytical_queries():
            query_indices.append(self._queries_map[query.raw_query.strip()])
        workload.set_predicted_data_access_statistics(
            aurora_pages=self._aurora_accessed_pages[query_indices],
            athena_bytes=self._athena_accessed_bytes[query_indices],
        )
