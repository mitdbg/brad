import grpc
from typing import Generator, Optional, AsyncGenerator

import brad.proto_gen.brad_pb2 as b
import brad.proto_gen.brad_pb2_grpc as brad_grpc
from brad.config.session import SessionId

# pylint: disable=no-member
# See https://github.com/protocolbuffers/protobuf/issues/10372


class AsyncBradGrpcClient:
    """
    An async wrapper over BRAD's gRPC stub, to simplify programmatic access through
    Python.

    This client is a thin wrapper over the gRPC stub. For application
    development, you will probably want to use the `BradGrpcClient` above.

    Methods on this client are synchronous.

    Usage:
    ```
    with AsyncBradGrpcClient(host, port) as client:
        session_id = client.start_session()
        for row in await client.run_query(session_id, "SELECT 1"):
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
        self._channel = grpc.aio.insecure_channel(
            "{}:{}".format(self._host, self._port)
        )
        self._stub = brad_grpc.BradStub(self._channel)

    def close(self) -> None:
        assert self._stub is not None
        assert self._channel is not None
        self._stub = None
        self._channel.close()
        self._channel = None

    async def start_session(self) -> SessionId:
        assert self._stub is not None
        result = await self._stub.StartSession(b.StartSessionRequest())
        return SessionId(result.id.id_value)

    async def end_session(self, session_id: SessionId) -> None:
        assert self._stub is not None
        await self._stub.EndSession(
            b.EndSessionRequest(id=b.SessionId(id_value=session_id.value()))
        )

    async def run_query(
        self, session_id: SessionId, query: str
    ) -> AsyncGenerator[bytes, None, None]:
        """
        Send a query to BRAD. The query result will come back row-by-row in
        encoded form. For simplicity, each row is currently encoded as a UTF-8
        string (meant only for printing to the screen).
        """
        assert self._stub is not None
        responses = self._stub.RunQuery(
            b.RunQueryRequest(id=b.SessionId(id_value=session_id.value()), query=query)
        )
        responses = responses.__aiter__()
        async for response_msg in responses:
            msg_kind = response_msg.WhichOneof("result")
            if msg_kind is None:
                raise BradClientError(
                    message="BRAD RPC error: Unspecified query result."
                )
            elif msg_kind == "error":
                raise BradClientError(message=response_msg.error.error_msg)
            elif msg_kind == "row":
                yield response_msg.row.row_data
            else:
                raise BradClientError(
                    message="BRAD RPC error: Unknown result message kind."
                )


class BradClientError(Exception):
    def __init__(self, message: str):
        self._message = message

    def message(self) -> str:
        return self._message
