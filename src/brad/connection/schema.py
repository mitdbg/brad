import enum
from typing import Tuple, Iterator, Iterable


class DataType(enum.Enum):
    """
    Represents the different data types that BRAD supports.

    In the near future, this will be more complex as we will explicitly start
    supporting different SQL dialects, which each have slightly different types.
    """

    # This is used when we cannot deduce the type from the underlying connection
    # (or we do not support it yet). The data should be treated as all `NULL`s.
    Unknown = 0

    Integer = 1
    Float = 2
    Decimal = 3  # Fixed precision.
    String = 4
    Timestamp = 5


class Field:
    """
    Represents the name and type of a column.
    """

    def __init__(self, name: str, data_type: DataType) -> None:
        self._name = name
        self._data_type = data_type

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_type(self) -> DataType:
        return self._data_type

    def __str__(self) -> str:
        return f"{self.name}: {str(self.data_type)}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Field):
            return False
        return self.name == other.name and self.data_type == other.data_type

    def __hash__(self) -> int:
        return hash((self.name, self.data_type))


class Schema:
    """
    Represents the name and type of the columns in a result set.
    """

    @classmethod
    def empty(cls) -> "Schema":
        return cls([])

    def __init__(self, fields: Iterable[Field]) -> None:
        self._fields = tuple(fields)

    @property
    def fields(self) -> Tuple[Field, ...]:
        return self._fields

    @property
    def num_fields(self) -> int:
        return len(self._fields)

    def __str__(self) -> str:
        return ", ".join([str(field) for field in self.fields])

    def __iter__(self) -> Iterator[Field]:
        return iter(self._fields)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Schema):
            return False
        return self.fields == other.fields

    def __hash__(self) -> int:
        return hash(self.fields)
