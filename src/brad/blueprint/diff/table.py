from typing import List

from brad.config.engine import Engine


class TableDiff:
    """
    Represents a change in a blueprint `Table`. We currently only support
    changes to a table's location. We do not support creating or dropping
    tables.
    """

    def __init__(
        self,
        table_name: str,
        added_locations: List[Engine],
        removed_locations: List[Engine],
    ) -> None:
        self._table_name = table_name
        self._added_locations = added_locations
        self._removed_locations = removed_locations

    def table_name(self) -> str:
        return self._table_name

    def added_locations(self) -> List[Engine]:
        return self._added_locations

    def removed_locations(self) -> List[Engine]:
        return self._removed_locations
