from typing import List
from .estimator import Estimator

from brad.data_stats.estimator import AccessInfo
from brad.query_rep import QueryRep


class StubEstimator(Estimator):
    """
    Returns dummy values. Meant for development use only.
    """

    async def get_access_info(self, query: QueryRep) -> List[AccessInfo]:
        return self.get_access_info_sync(query)

    def get_access_info_sync(self, query: QueryRep) -> List[AccessInfo]:
        return [
            AccessInfo(
                table_name=table,
                cardinality=10,
                selectivity=0.2,
                width=10,
                op_name="StubScan",
            )
            for table in query.tables()
        ]
