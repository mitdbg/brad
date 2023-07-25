from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

ATHENA: Engine
AURORA: Engine
DESCRIPTOR: _descriptor.FileDescriptor
REDSHIFT: Engine
UNKNOWN: Engine

class Blueprint(_message.Message):
    __slots__ = ["aurora", "policy", "redshift", "schema_name", "tables"]
    AURORA_FIELD_NUMBER: _ClassVar[int]
    POLICY_FIELD_NUMBER: _ClassVar[int]
    REDSHIFT_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    aurora: Provisioning
    policy: RoutingPolicy
    redshift: Provisioning
    schema_name: str
    tables: _containers.RepeatedCompositeFieldContainer[Table]
    def __init__(self, schema_name: _Optional[str] = ..., tables: _Optional[_Iterable[_Union[Table, _Mapping]]] = ..., aurora: _Optional[_Union[Provisioning, _Mapping]] = ..., redshift: _Optional[_Union[Provisioning, _Mapping]] = ..., policy: _Optional[_Union[RoutingPolicy, _Mapping]] = ...) -> None: ...

class Index(_message.Message):
    __slots__ = ["column_name"]
    COLUMN_NAME_FIELD_NUMBER: _ClassVar[int]
    column_name: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, column_name: _Optional[_Iterable[str]] = ...) -> None: ...

class Provisioning(_message.Message):
    __slots__ = ["instance_type", "num_nodes"]
    INSTANCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    NUM_NODES_FIELD_NUMBER: _ClassVar[int]
    instance_type: str
    num_nodes: int
    def __init__(self, instance_type: _Optional[str] = ..., num_nodes: _Optional[int] = ...) -> None: ...

class RoutingPolicy(_message.Message):
    __slots__ = ["policy"]
    POLICY_FIELD_NUMBER: _ClassVar[int]
    policy: bytes
    def __init__(self, policy: _Optional[bytes] = ...) -> None: ...

class Table(_message.Message):
    __slots__ = ["columns", "dependencies", "indexes", "locations", "table_name"]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    DEPENDENCIES_FIELD_NUMBER: _ClassVar[int]
    INDEXES_FIELD_NUMBER: _ClassVar[int]
    LOCATIONS_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedCompositeFieldContainer[TableColumn]
    dependencies: TableDependency
    indexes: _containers.RepeatedCompositeFieldContainer[Index]
    locations: _containers.RepeatedScalarFieldContainer[Engine]
    table_name: str
    def __init__(self, table_name: _Optional[str] = ..., columns: _Optional[_Iterable[_Union[TableColumn, _Mapping]]] = ..., locations: _Optional[_Iterable[_Union[Engine, str]]] = ..., dependencies: _Optional[_Union[TableDependency, _Mapping]] = ..., indexes: _Optional[_Iterable[_Union[Index, _Mapping]]] = ...) -> None: ...

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

class Engine(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):  # type: ignore
    __slots__ = []  # type: ignore
