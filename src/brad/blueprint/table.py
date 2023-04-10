from typing import List, Optional

from brad.config.engine import Engine


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


class Table:
    """
    Holds metadata that BRAD needs to know about a table:
    - Its schema (name, columns, primary key columns)
    - The engines on which the table is located
    - Its dependencies
    - The transformation to use when propagating changes from the dependencies
    """

    def __init__(
        self,
        name: str,
        columns: List[Column],
        table_dependencies: List[str],
        transform_text: Optional[str],
        locations: List[Engine],
    ):
        self._name = name
        self._columns = columns
        self._table_dependencies = table_dependencies
        self._transform_text = transform_text
        self._locations = locations
        self._primary_key = list(filter(lambda c: c.is_primary, columns))

    @property
    def name(self) -> str:
        return self._name

    @property
    def columns(self) -> List[Column]:
        return self._columns

    @property
    def table_dependencies(self) -> List[str]:
        return self._table_dependencies

    @property
    def transform_text(self) -> Optional[str]:
        return self._transform_text

    @property
    def locations(self) -> List[Engine]:
        return self._locations

    @property
    def primary_key(self) -> List[Column]:
        return self._primary_key
