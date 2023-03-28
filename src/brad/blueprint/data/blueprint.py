from .location import Location
from .table import TableSchema, TableLocation, TableDependency

from typing import Dict, List, Optional


class DataBlueprint:
    def __init__(
        self,
        db_name: str,
        table_schemas: List[TableSchema],
        table_locations: List[TableLocation],
        table_dependencies: List[TableDependency],
    ):
        self._db_name = db_name
        self._table_schemas = table_schemas
        self._table_locations = table_locations
        self._table_dependencies = table_dependencies

        self._schemas_by_name = {tbl.name: tbl for tbl in self._table_schemas}
        self._table_locations_by_name: Dict[str, List[Location]] = dict()
        for tbl_loc in self._table_locations:
            if tbl_loc.table_name not in self._table_locations_by_name:
                self._table_locations_by_name[tbl_loc.table_name] = [tbl_loc.location]
            else:
                self._table_locations_by_name[tbl_loc.table_name].append(
                    tbl_loc.location
                )
        self._dependencies_by_target = {dep.target: dep for dep in table_dependencies}

    @property
    def db_name(self) -> str:
        return self._db_name

    @property
    def table_schemas(self) -> List[TableSchema]:
        return self._table_schemas

    @property
    def table_locations(self) -> List[TableLocation]:
        return self._table_locations

    @property
    def table_dependencies(self) -> List[TableDependency]:
        return self._table_dependencies

    def table_names(self) -> List[str]:
        return list(self._schemas_by_name.keys())

    def schema_for(self, table_name: str) -> TableSchema:
        return self._schemas_by_name[table_name]

    def locations_of(self, table_name: str) -> List[Location]:
        return self._table_locations_by_name[table_name]

    def dependencies_of(self, table: TableLocation) -> Optional[TableDependency]:
        if table not in self._dependencies_by_target:
            return None
        return self._dependencies_by_target[table]
