from typing import Tuple, Set

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.planner.workload import Workload
from .filter import Filter


class SingleEngineExecution(Filter):
    """
    Ensures that all tables referenced by a query are present together on at
    least one engine.
    """

    def __init__(self, workload: Workload) -> None:
        # Each constraint is a set of tables that must appear together on at
        # least one engine.
        constraint_set: Set[Tuple[str, ...]] = set()
        for query in workload.all_queries():
            clone = tuple(query.tables())
            constraint_set.add(clone)
        self._table_constraints = list(constraint_set)

    def is_valid(self, candidate: Blueprint) -> bool:
        table_locations = candidate.table_locations_bitmap()

        for constraint in self._table_constraints:
            locations = Engine.bitmap_all()
            for tbl in constraint:
                locations &= table_locations[tbl]
                if locations == 0:
                    return False
        return True
