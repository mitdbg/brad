from brad.blueprint import Blueprint

from .filter import Filter


class NoDataLoss(Filter):
    """
    Ensures that every table is present on at least one engine.
    """

    def is_valid(self, candidate: Blueprint) -> bool:
        for locations in candidate.table_locations().values():
            if len(locations) == 0:
                return False
        return True
