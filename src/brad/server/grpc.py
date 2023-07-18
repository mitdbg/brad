from typing import AsyncIterable, Dict, Any

import brad.proto_gen.brad_pb2 as b
import brad.proto_gen.brad_pb2_grpc as rpc
from brad.config.engine import Engine
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
        self, _request: b.StartSessionRequest, _context
    ) -> b.StartSessionResponse:
        new_session_id = await self._brad.start_session()
        return b.StartSessionResponse(id=b.SessionId(id_value=new_session_id.value()))

    async def RunQuery(
        self, request: b.RunQueryRequest, _context
    ) -> AsyncIterable[b.RunQueryResponse]:
        session_id = SessionId(request.id.id_value)
        debug_info: Dict[str, Any] = {}
        try:
            async for row in self._brad.run_query(
                session_id, request.query, debug_info
            ):
                response = b.RunQueryResponse(row=b.QueryResultRow(row_data=row))
                if "executor" in debug_info:
                    response.executor = self._convert_engine(debug_info["executor"])
                yield response

        except QueryError as ex:
            yield b.RunQueryResponse(error=b.QueryError(error_msg=str(ex)))

    async def RunQueryJson(
        self, request: b.RunQueryRequest, _context
    ) -> b.RunQueryJsonResponse:
        session_id = SessionId(request.id.id_value)
        debug_info: Dict[str, Any] = {}
        try:
            result = await self._brad.run_query_json(
                session_id, request.query, debug_info
            )
            return b.RunQueryJsonResponse(
                results=b.QueryJsonResponse(
                    results_json=result,
                    executor=self._convert_engine(debug_info["executor"]),
                )
            )

        except QueryError as ex:
            return b.RunQueryJsonResponse(error=b.QueryError(error_msg=str(ex)))

    async def EndSession(
        self, request: b.EndSessionRequest, _context
    ) -> b.EndSessionResponse:
        session_id = SessionId(request.id.id_value)
        await self._brad.end_session(session_id)
        return b.EndSessionResponse()

    def _convert_engine(self, engine: Engine) -> b.ExecutionEngine:
        if engine == Engine.Aurora:
            return b.ENG_AURORA
        elif engine == Engine.Redshift:
            return b.ENG_REDSHIFT
        elif engine == Engine.Athena:
            return b.ENG_ATHENA
        else:
            raise ValueError("Unknown engine: {}".format(engine.value))
