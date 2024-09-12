from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Engine(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []  # type: ignore
    ENGINE_UNKNOWN: _ClassVar[Engine]
    ENGINE_AURORA: _ClassVar[Engine]
    ENGINE_REDSHIFT: _ClassVar[Engine]
    ENGINE_ATHENA: _ClassVar[Engine]
ENGINE_UNKNOWN: Engine
ENGINE_AURORA: Engine
ENGINE_REDSHIFT: Engine
ENGINE_ATHENA: Engine

class Blueprint(_message.Message):
    __slots__ = ["aurora", "redshift", "policy"]
    AURORA_FIELD_NUMBER: _ClassVar[int]
    REDSHIFT_FIELD_NUMBER: _ClassVar[int]
    POLICY_FIELD_NUMBER: _ClassVar[int]
    aurora: Provisioning
    redshift: Provisioning
    policy: RoutingPolicy
    def __init__(self, aurora: _Optional[_Union[Provisioning, _Mapping]] = ..., redshift: _Optional[_Union[Provisioning, _Mapping]] = ..., policy: _Optional[_Union[RoutingPolicy, _Mapping]] = ...) -> None: ...

class RoutingPolicy(_message.Message):
    __slots__ = ["policy"]
    POLICY_FIELD_NUMBER: _ClassVar[int]
    policy: bytes
    def __init__(self, policy: _Optional[bytes] = ...) -> None: ...

class PhysicalSnapshot(_message.Message):
    __slots__ = ["vdbes", "tables", "location"]
    VDBES_FIELD_NUMBER: _ClassVar[int]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    vdbes: _containers.RepeatedScalarFieldContainer[str]
    tables: _containers.RepeatedScalarFieldContainer[str]
    location: Engine
    def __init__(self, vdbes: _Optional[_Iterable[str]] = ..., tables: _Optional[_Iterable[str]] = ..., location: _Optional[_Union[Engine, str]] = ...) -> None: ...

class Provisioning(_message.Message):
    __slots__ = ["instance_type", "num_nodes"]
    INSTANCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    NUM_NODES_FIELD_NUMBER: _ClassVar[int]
    instance_type: str
    num_nodes: int
    def __init__(self, instance_type: _Optional[str] = ..., num_nodes: _Optional[int] = ...) -> None: ...
