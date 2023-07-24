from typing import List, Optional, Tuple


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

    def clone(self) -> "Column":
        return Column(self._name, self._data_type, self._is_primary)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Column):
            return False
        return (
            self.name == other.name
            and self.data_type == other.data_type
            and self.is_primary == other.is_primary
        )

    def __hash__(self) -> int:
        return hash((self.name, self.data_type))


class Table:
    """
    Holds metadata that BRAD needs to know about a table:
    - Its schema (name, columns, primary key columns)
    - The engines on which the table is located
    - Its dependencies
    - The transformation to use when propagating changes from the dependencies
    - Columns (aside from the primary key) that are indexed
    """

    def __init__(
        self,
        name: str,
        columns: List[Column],
        table_dependencies: List[str],
        transform_text: Optional[str],
        secondary_indexed_columns: List[Tuple[Column, ...]],
    ):
        self._name = name
        self._columns = columns
        self._table_dependencies = table_dependencies
        self._transform_text = transform_text
        self._primary_key = list(filter(lambda c: c.is_primary, columns))
        self._secondary_indexed_columns = secondary_indexed_columns

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
    def primary_key(self) -> List[Column]:
        return self._primary_key

    @property
    def secondary_indexed_columns(self) -> List[Tuple[Column, ...]]:
        return self._secondary_indexed_columns

    def set_secondary_indexed_columns(self, indexes: List[Tuple[Column, ...]]) -> None:
        self._secondary_indexed_columns.clear()
        self._secondary_indexed_columns.extend(indexes)

    def clone(self) -> "Table":
        return Table(
            self._name,
            [col.clone() for col in self._columns],
            self._table_dependencies.copy(),
            self._transform_text,
            self._secondary_indexed_columns.copy(),
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Table):
            return False
        return (
            self.name == other.name
            and self.columns == other.columns
            and self.table_dependencies == other.table_dependencies
            and self.transform_text == other.transform_text
            and self._secondary_indexed_columns == other._secondary_indexed_columns
        )
