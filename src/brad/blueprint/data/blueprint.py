from .location import Location
from .table import TableSchema, TableLocation, TableDependency

from typing import Dict, List


class DataBlueprint:
    def __init__(
        self,
        db_name: str,
        table_schemas: List[TableSchema],
        table_locations: List[TableLocation],
        dependencies: List[TableDependency],
    ):
        self._db_name = db_name
        self._schemas_by_name = {tbl.name: tbl for tbl in table_schemas}
        self._table_locations: Dict[str, List[Location]] = dict()
        for tbl_loc in table_locations:
            if tbl_loc.table_name not in self._table_locations:
                self._table_locations[tbl_loc.table_name] = [tbl_loc.location]
            else:
                self._table_locations[tbl_loc.table_name].append(tbl_loc.location)
        self._dependencies = dependencies

    def table_names(self) -> List[str]:
        return list(self._schemas_by_name.keys())

    def schema_for(self, table_name: str) -> TableSchema:
        return self._schemas_by_name[table_name]

    def locations_of(self, table_name: str) -> List[Location]:
        return self._table_locations[table_name]
