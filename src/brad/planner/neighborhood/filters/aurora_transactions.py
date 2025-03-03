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
        for query in workload.transactional_queries():
            self._transactional_tables.update(query.tables())

    def is_valid(self, candidate: Blueprint) -> bool:
        for table in candidate.tables():
            if (
                table.name in self._transactional_tables
                and Engine.Aurora not in candidate.get_table_locations(table.name)
            ):
                return False
        return True
