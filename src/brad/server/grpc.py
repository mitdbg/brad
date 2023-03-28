from typing import AsyncIterable

import brad.proto_gen.brad_pb2 as b
import brad.proto_gen.brad_pb2_grpc as rpc
from brad.config.session import SessionId
from brad.server.brad_interface import BradInterface
from brad.server.errors import QueryError

# pylint: disable=no-member
# See https://github.com/protocolbuffers/protobuf/issues/10372

# pylint: disable=invalid-overridden-method


class BradGrpc(rpc.BradServicer):
    """
    A shim layer used to implement BRAD's gRPC interface.
    """

    def __init__(self, brad: BradInterface):
        self._brad = brad

    async def StartSession(
        self, request: b.StartSessionRequest, _context
    ) -> b.StartSessionResponse:
        new_session_id = await self._brad.start_session(request.database_name)
        return b.StartSessionResponse(id=b.SessionId(id_value=new_session_id.value()))

    async def RunQuery(
        self, request: b.RunQueryRequest, _context
    ) -> AsyncIterable[b.RunQueryResponse]:
        session_id = SessionId(request.id.id_value)
        try:
            async for row in self._brad.run_query(session_id, request.query):
                yield b.RunQueryResponse(row=b.QueryResultRow(row_data=row))
        except QueryError as ex:
            yield b.RunQueryResponse(error=b.QueryError(error_msg=str(ex)))

    async def EndSession(
        self, request: b.EndSessionRequest, _context
    ) -> b.EndSessionResponse:
        session_id = SessionId(request.id.id_value)
        await self._brad.end_session(session_id)
        return b.EndSessionResponse()
