from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.planner.workload import Workload
from .filter import Filter


class AuroraTransactions(Filter):
    """
    Ensures that any tables accessed transactionally are present on Aurora.
    """

    def __init__(self, workload: Workload) -> None:
        self._transactional_tables = set()
        for template in workload.templates():
            if not template.is_transactional():
                continue
            self._transactional_tables.update(template.tables())

    def is_valid(self, candidate: Blueprint) -> bool:
        for table in candidate.tables():
            if (
                table.name in self._transactional_tables
                and Engine.Aurora not in table.locations
            ):
                return False
        return True
