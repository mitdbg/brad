from interface import blueprint_pb2 as _blueprint_pb2
from interface import schema_pb2 as _schema_pb2
from interface import vdbe_pb2 as _vdbe_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SystemState(_message.Message):
    __slots__ = ["schema_name", "tables", "vdbes", "blueprint"]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    VDBES_FIELD_NUMBER: _ClassVar[int]
    BLUEPRINT_FIELD_NUMBER: _ClassVar[int]
    schema_name: str
    tables: _containers.RepeatedCompositeFieldContainer[_schema_pb2.Table]
    vdbes: _containers.RepeatedCompositeFieldContainer[_vdbe_pb2.VirtualEngine]
    blueprint: _blueprint_pb2.Blueprint
    def __init__(self, schema_name: _Optional[str] = ..., tables: _Optional[_Iterable[_Union[_schema_pb2.Table, _Mapping]]] = ..., vdbes: _Optional[_Iterable[_Union[_vdbe_pb2.VirtualEngine, _Mapping]]] = ..., blueprint: _Optional[_Union[_blueprint_pb2.Blueprint, _Mapping]] = ...) -> None: ...
