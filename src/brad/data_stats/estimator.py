from collections import namedtuple
from typing import List

from brad.query_rep import QueryRep


AccessInfo = namedtuple("AccessInfo", ["table_name", "cardinality", "width", "op_name"])


class Estimator:
    """
    Abstract interface representing an object that can provide estimated
    statistics for a query. In practice, this object is used for cardinality
    estimation for base table accesses (how many rows are we estimated to
    select, based on the filter predicates applied to the base table).
    """

    async def analyze(self) -> None:
        """
        Gathers any statistics needed by the estimator.
        """
        pass

    async def get_access_info(self, query: QueryRep) -> List[AccessInfo]:
        raise NotImplementedError
