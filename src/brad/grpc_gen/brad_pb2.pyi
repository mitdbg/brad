from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EndSessionRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: SessionId
    def __init__(self, id: _Optional[_Union[SessionId, _Mapping]] = ...) -> None: ...

class EndSessionResponse(_message.Message):
    __slots__ = ["unused"]
    UNUSED_FIELD_NUMBER: _ClassVar[int]
    unused: int
    def __init__(self, unused: _Optional[int] = ...) -> None: ...

class QueryError(_message.Message):
    __slots__ = ["error_msg"]
    ERROR_MSG_FIELD_NUMBER: _ClassVar[int]
    error_msg: str
    def __init__(self, error_msg: _Optional[str] = ...) -> None: ...

class QueryResultRow(_message.Message):
    __slots__ = ["row_data"]
    ROW_DATA_FIELD_NUMBER: _ClassVar[int]
    row_data: bytes
    def __init__(self, row_data: _Optional[bytes] = ...) -> None: ...

class RunQueryRequest(_message.Message):
    __slots__ = ["id", "query"]
    ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    id: SessionId
    query: str
    def __init__(self, id: _Optional[_Union[SessionId, _Mapping]] = ..., query: _Optional[str] = ...) -> None: ...

class RunQueryResponse(_message.Message):
    __slots__ = ["error", "row"]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ROW_FIELD_NUMBER: _ClassVar[int]
    error: QueryError
    row: QueryResultRow
    def __init__(self, row: _Optional[_Union[QueryResultRow, _Mapping]] = ..., error: _Optional[_Union[QueryError, _Mapping]] = ...) -> None: ...

class SessionId(_message.Message):
    __slots__ = ["id_value"]
    ID_VALUE_FIELD_NUMBER: _ClassVar[int]
    id_value: int
    def __init__(self, id_value: _Optional[int] = ...) -> None: ...

class StartSessionRequest(_message.Message):
    __slots__ = ["unused"]
    UNUSED_FIELD_NUMBER: _ClassVar[int]
    unused: int
    def __init__(self, unused: _Optional[int] = ...) -> None: ...

class StartSessionResponse(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: SessionId
    def __init__(self, id: _Optional[_Union[SessionId, _Mapping]] = ...) -> None: ...
