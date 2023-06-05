from brad.blueprint import Blueprint
from brad.config.engine import Engine

from .filter import Filter


class TableOnEngine(Filter):
    """
    Ensures that a table is only placed on an engine if it has at least one node
    (or is Athena).
    """

    def is_valid(self, candidate: Blueprint) -> bool:
        has_aurora = candidate.aurora_provisioning().num_nodes() > 0
        has_redshift = candidate.redshift_provisioning().num_nodes() > 0

        for table in candidate.tables():
            table_locations = candidate.get_table_locations(table.name)
            if not has_aurora and Engine.Aurora in table_locations:
                return False
            if not has_redshift and Engine.Redshift in table_locations:
                return False

        return True
