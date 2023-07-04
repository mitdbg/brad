from collections import namedtuple
from typing import List, TYPE_CHECKING

from brad.query_rep import QueryRep

# Needed to avoid a circular import.
if TYPE_CHECKING:
    from brad.blueprint.blueprint import Blueprint


AccessInfo = namedtuple(
    "AccessInfo", ["table_name", "cardinality", "selectivity", "width", "op_name"]
)


class Estimator:
    """
    Abstract interface representing an object that can provide estimated
    statistics for a query. In practice, this object is used for cardinality
    estimation for base table accesses (how many rows are we estimated to
    select, based on the filter predicates applied to the base table).
    """

    async def analyze(self, blueprint: "Blueprint") -> None:
        """
        Gathers any statistics needed by the estimator.
        """

    async def get_access_info(self, query: QueryRep) -> List[AccessInfo]:
        """
        Estimates statistics about the provided query.
        """
        raise NotImplementedError

    def get_access_info_sync(self, query: QueryRep) -> List[AccessInfo]:
        """
        Estimates statistics about the provided query.
        """
        raise NotImplementedError

    async def close(self) -> None:
        """
        Performs any cleanup tasks when shutting down the estimator.
        """
