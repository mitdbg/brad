from brad.blueprint import Blueprint
from brad.planner.workload import Workload
from .filter import Filter


class SingleEngineExecution(Filter):
    """
    Ensures that all tables referenced by a query are present together on at
    least one engine.
    """

    def __init__(self, workload: Workload) -> None:
        # Each constraint is a list of tables that must appear together on at
        # least one engine.
        self._table_constraints = [
            template.tables() for template in workload.templates()
        ]

    def is_valid(self, candidate: Blueprint) -> bool:
        table_locations = {tbl.name: set(tbl.locations) for tbl in candidate.tables()}

        for constraint in self._table_constraints:
            sets = map(lambda tbl: table_locations[tbl], constraint)
            intersection = set.intersection(*sets)
            if len(intersection) < 1:
                return False

        return True
