from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

AURORA: DataLocation
DESCRIPTOR: _descriptor.FileDescriptor
REDSHIFT: DataLocation
S3_ICEBERG: DataLocation
UNKNOWN: DataLocation

class DataBlueprint(_message.Message):
    __slots__ = ["schema_name", "tables"]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    schema_name: str
    tables: _containers.RepeatedCompositeFieldContainer[Table]
    def __init__(self, schema_name: _Optional[str] = ..., tables: _Optional[_Iterable[_Union[Table, _Mapping]]] = ...) -> None: ...

class Table(_message.Message):
    __slots__ = ["columns", "dependencies", "locations", "table_name"]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    DEPENDENCIES_FIELD_NUMBER: _ClassVar[int]
    LOCATIONS_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedCompositeFieldContainer[TableColumn]
    dependencies: TableDependency
    locations: _containers.RepeatedScalarFieldContainer[DataLocation]
    table_name: str
    def __init__(self, table_name: _Optional[str] = ..., columns: _Optional[_Iterable[_Union[TableColumn, _Mapping]]] = ..., locations: _Optional[_Iterable[_Union[DataLocation, str]]] = ..., dependencies: _Optional[_Union[TableDependency, _Mapping]] = ...) -> None: ...

class TableColumn(_message.Message):
    __slots__ = ["data_type", "is_primary", "name"]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    IS_PRIMARY_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    data_type: str
    is_primary: bool
    name: str
    def __init__(self, name: _Optional[str] = ..., data_type: _Optional[str] = ..., is_primary: bool = ...) -> None: ...

class TableDependency(_message.Message):
    __slots__ = ["source_table_names", "transform"]
    SOURCE_TABLE_NAMES_FIELD_NUMBER: _ClassVar[int]
    TRANSFORM_FIELD_NUMBER: _ClassVar[int]
    source_table_names: _containers.RepeatedScalarFieldContainer[str]
    transform: str
    def __init__(self, source_table_names: _Optional[_Iterable[str]] = ..., transform: _Optional[str] = ...) -> None: ...

class DataLocation(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):  # type: ignore
    __slots__ = []  # type: ignore
