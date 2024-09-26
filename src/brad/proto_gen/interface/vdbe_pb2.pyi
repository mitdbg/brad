from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class QueryInterface(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []  # type: ignore
    QI_UNKNOWN: _ClassVar[QueryInterface]
    QI_SQL_POSTGRESQL: _ClassVar[QueryInterface]
    QI_SQL_MYSQL: _ClassVar[QueryInterface]
    QI_SQL_AWS_REDSHIFT: _ClassVar[QueryInterface]
    QI_SQL_AWS_ATHENA: _ClassVar[QueryInterface]
QI_UNKNOWN: QueryInterface
QI_SQL_POSTGRESQL: QueryInterface
QI_SQL_MYSQL: QueryInterface
QI_SQL_AWS_REDSHIFT: QueryInterface
QI_SQL_AWS_ATHENA: QueryInterface

class VirtualEngine(_message.Message):
    __slots__ = ["name", "qi", "tables", "max_staleness_ms"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    QI_FIELD_NUMBER: _ClassVar[int]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    MAX_STALENESS_MS_FIELD_NUMBER: _ClassVar[int]
    name: str
    qi: QueryInterface
    tables: _containers.RepeatedCompositeFieldContainer[VirtualTable]
    max_staleness_ms: int
    def __init__(self, name: _Optional[str] = ..., qi: _Optional[_Union[QueryInterface, str]] = ..., tables: _Optional[_Iterable[_Union[VirtualTable, _Mapping]]] = ..., max_staleness_ms: _Optional[int] = ...) -> None: ...

class VirtualTable(_message.Message):
    __slots__ = ["name", "writable"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    WRITABLE_FIELD_NUMBER: _ClassVar[int]
    name: str
    writable: bool
    def __init__(self, name: _Optional[str] = ..., writable: bool = ...) -> None: ...
