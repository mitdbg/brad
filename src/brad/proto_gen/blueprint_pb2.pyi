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
    __slots__ = ["schema_name", "table_dependencies", "table_locations", "table_schemas"]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_DEPENDENCIES_FIELD_NUMBER: _ClassVar[int]
    TABLE_LOCATIONS_FIELD_NUMBER: _ClassVar[int]
    TABLE_SCHEMAS_FIELD_NUMBER: _ClassVar[int]
    schema_name: str
    table_dependencies: _containers.RepeatedCompositeFieldContainer[TableDependency]
    table_locations: _containers.RepeatedCompositeFieldContainer[TableLocation]
    table_schemas: _containers.RepeatedCompositeFieldContainer[TableSchema]
    def __init__(self, schema_name: _Optional[str] = ..., table_schemas: _Optional[_Iterable[_Union[TableSchema, _Mapping]]] = ..., table_locations: _Optional[_Iterable[_Union[TableLocation, _Mapping]]] = ..., table_dependencies: _Optional[_Iterable[_Union[TableDependency, _Mapping]]] = ...) -> None: ...

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
    __slots__ = ["sources", "target", "transform"]
    SOURCES_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    TRANSFORM_FIELD_NUMBER: _ClassVar[int]
    sources: _containers.RepeatedCompositeFieldContainer[TableLocation]
    target: TableLocation
    transform: str
    def __init__(self, target: _Optional[_Union[TableLocation, _Mapping]] = ..., sources: _Optional[_Iterable[_Union[TableLocation, _Mapping]]] = ..., transform: _Optional[str] = ...) -> None: ...

class TableLocation(_message.Message):
    __slots__ = ["location", "table_name"]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    location: DataLocation
    table_name: str
    def __init__(self, table_name: _Optional[str] = ..., location: _Optional[_Union[DataLocation, str]] = ...) -> None: ...

class TableSchema(_message.Message):
    __slots__ = ["columns", "table_name"]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedCompositeFieldContainer[TableColumn]
    table_name: str
    def __init__(self, table_name: _Optional[str] = ..., columns: _Optional[_Iterable[_Union[TableColumn, _Mapping]]] = ...) -> None: ...

class DataLocation(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):  # type: ignore
    __slots__ = []  # type: ignore
