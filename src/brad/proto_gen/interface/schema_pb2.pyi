from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DataType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []  # type: ignore
    DT_UNKNOWN: _ClassVar[DataType]
    DT_INT_32: _ClassVar[DataType]
    DT_INT_64: _ClassVar[DataType]
    DT_STRING: _ClassVar[DataType]
DT_UNKNOWN: DataType
DT_INT_32: DataType
DT_INT_64: DataType
DT_STRING: DataType

class Table(_message.Message):
    __slots__ = ["name", "columns"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    name: str
    columns: _containers.RepeatedCompositeFieldContainer[TableColumn]
    def __init__(self, name: _Optional[str] = ..., columns: _Optional[_Iterable[_Union[TableColumn, _Mapping]]] = ...) -> None: ...

class TableColumn(_message.Message):
    __slots__ = ["name", "type", "nullable"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    NULLABLE_FIELD_NUMBER: _ClassVar[int]
    name: str
    type: DataType
    nullable: bool
    def __init__(self, name: _Optional[str] = ..., type: _Optional[_Union[DataType, str]] = ..., nullable: bool = ...) -> None: ...
