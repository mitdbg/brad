from .location import Location
from typing import List, Optional


class Column:
    """
    Represents a column (its name and type).
    """

    def __init__(self, name: str, data_type: str, is_primary: bool):
        self._name = name
        self._data_type = data_type
        self._is_primary = is_primary

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def is_primary(self) -> bool:
        return self._is_primary


class TableSchema:
    """
    Represents a table's schema (its name, columns, and primary key columns).
    """

    def __init__(
        self,
        name: str,
        columns: List[Column],
    ):
        self._name = name
        self._columns = columns
        self._primary_key = list(filter(lambda c: c.is_primary, columns))

    @property
    def name(self) -> str:
        return self._name

    @property
    def columns(self) -> List[Column]:
        return self._columns

    @property
    def primary_key(self) -> List[Column]:
        return self._primary_key


class UserProvidedTable(TableSchema):
    """
    Extends a `TableSchema` with additional properties that are provided by a
    user (table dependencies and transforms).
    """

    def __init__(
        self,
        name: str,
        columns: List[Column],
        table_dependencies: List[str],
        transform_text: Optional[str],
    ):
        super().__init__(name, columns)
        self._table_dependencies = table_dependencies
        self._transform_text = transform_text

    @property
    def table_dependencies(self) -> List[str]:
        return self._table_dependencies

    @property
    def transform_text(self) -> Optional[str]:
        return self._transform_text

    def as_schema(self) -> TableSchema:
        return self


class TableLocation:
    """
    Indicates that the table with a name of `table_name` is stored in
    `location`.
    """

    def __init__(self, table_name: str, location: Location):
        self._table_name = table_name
        self._location = location

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def location(self) -> Location:
        return self._location


class TableDependency:
    def __init__(
        self,
        sources: List[TableLocation],
        target: TableLocation,
        transform: Optional[str],
    ):
        self._sources = sources
        self._target = target
        self._transform = transform
