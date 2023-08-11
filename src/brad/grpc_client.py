import grpc
import json
from typing import Generator, Optional, Tuple, List, Any

import brad.proto_gen.brad_pb2 as b
import brad.proto_gen.brad_pb2_grpc as brad_grpc
from brad.config.engine import Engine
from brad.config.session import SessionId

# pylint: disable=no-member
# See https://github.com/protocolbuffers/protobuf/issues/10372

RowList = List[Tuple[Any, ...]]


class BradGrpcClient:
    """
    A client that communicates with BRAD over its gRPC interface.

    Usage:
    ```
    with BradGrpcClient(host, port) as client:
        for row in client.run_query(session_id, "SELECT 1"):
            print(row)
    ```
    """

    def __init__(self, host: str, port: int):
        self._impl = BradRawGrpcClient(host, port)
        self._session_id: Optional[SessionId] = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self):
        self._impl.connect()
        self._session_id = self._impl.start_session()

    def close(self):
        assert self._session_id is not None
        self._impl.end_session(self._session_id)
        self._impl.close()
        self._session_id = None

    def run_query(
        self, query: str
    ) -> Generator[Tuple[bytes, Optional[Engine]], None, None]:
        """
        Send a query to BRAD. The query result will come back row-by-row in
        encoded form. For simplicity, each row is currently encoded as a UTF-8
        string (meant only for printing to the screen).
        """
        assert self._session_id is not None
        for row in self._impl.run_query(self._session_id, query):
            yield row

    def run_query_json(self, query: str) -> Tuple[RowList, Optional[Engine]]:
        assert self._session_id is not None
        return self._impl.run_query_json(self._session_id, query)

    def run_query_ignore_results(self, query: str) -> None:
        """
        Sends a query to BRAD and pulls out the results without returning them.
        """
        assert self._session_id is not None
        self._impl.run_query_json(self._session_id, query, ignore_results=True)


class BradRawGrpcClient:
    """
    A wrapper over BRAD's gRPC stub, to simplify programmatic access through
    Python.

    This client is a thin wrapper over the gRPC stub. For application
    development, you will probably want to use the `BradGrpcClient` above.

    Methods on this client are synchronous.

    Usage:
    ```
    with BradGrpcClient(host, port) as client:
        session_id = client.start_session()
        for row in client.run_query(session_id, "SELECT 1"):
            print(row)
        client.end_session(session_id)
    ```
    """

    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port
        self._channel = None
        self._stub: Optional[brad_grpc.BradStub] = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self) -> None:
        self._channel = grpc.insecure_channel("{}:{}".format(self._host, self._port))
        self._stub = brad_grpc.BradStub(self._channel)

    def close(self) -> None:
        assert self._stub is not None
        assert self._channel is not None
        self._stub = None
        self._channel.close()
        self._channel = None

    def start_session(self) -> SessionId:
        assert self._stub is not None
        result = self._stub.StartSession(b.StartSessionRequest())
        return SessionId(result.id.id_value)

    def end_session(self, session_id: SessionId) -> None:
        assert self._stub is not None
        self._stub.EndSession(
            b.EndSessionRequest(id=b.SessionId(id_value=session_id.value()))
        )

    def run_query(
        self, session_id: SessionId, query: str
    ) -> Generator[Tuple[bytes, Optional[Engine]], None, None]:
        """
        Send a query to BRAD. The query result will come back row-by-row in
        encoded form. For simplicity, each row is currently encoded as a UTF-8
        string (meant only for printing to the screen).
        """
        assert self._stub is not None
        responses = self._stub.RunQuery(
            b.RunQueryRequest(id=b.SessionId(id_value=session_id.value()), query=query)
        )
        for response_msg in responses:
            msg_kind = response_msg.WhichOneof("result")
            if msg_kind is None:
                raise BradClientError(
                    message="BRAD RPC error: Unspecified query result."
                )
            elif msg_kind == "error":
                raise BradClientError(
                    message=response_msg.error.error_msg,
                    is_transient=response_msg.error.is_transient,
                )
            elif msg_kind == "row":
                executor = response_msg.executor
                yield (response_msg.row.row_data, self._convert_engine(executor))
            else:
                raise BradClientError(
                    message="BRAD RPC error: Unknown result message kind."
                )

    def run_query_json(
        self, session_id: SessionId, query: str, ignore_results: bool = False
    ) -> Tuple[RowList, Optional[Engine]]:
        assert self._stub is not None
        response = self._stub.RunQueryJson(
            b.RunQueryRequest(id=b.SessionId(id_value=session_id.value()), query=query)
        )
        msg_kind = response.WhichOneof("result")
        if msg_kind is None:
            raise BradClientError(message="BRAD RPC error: Unspecified query result.")
        elif msg_kind == "error":
            raise BradClientError(
                message=response.error.error_msg,
                is_transient=response.error.is_transient,
            )
        elif msg_kind == "results":
            executor = response.results.executor
            if ignore_results:
                return ([], executor)
            else:
                return (
                    json.loads(response.results.results_json),
                    self._convert_engine(executor),
                )
        else:
            raise BradClientError(
                message="BRAD RPC error: Unknown result message kind."
            )

    def _convert_engine(self, engine: b.ExecutionEngine) -> Optional[Engine]:
        if engine == b.ENG_AURORA:
            return Engine.Aurora
        elif engine == b.ENG_REDSHIFT:
            return Engine.Redshift
        elif engine == b.ENG_ATHENA:
            return Engine.Athena
        else:
            return None


class BradClientError(Exception):
    def __init__(self, message: str, is_transient: bool = False):
        self._message = message
        self._is_transient = is_transient

    def message(self) -> str:
        return self._message

    def is_transient(self) -> bool:
        return self._is_transient

    def __repr__(self) -> str:
        return self._message
