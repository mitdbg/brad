import grpc
import json
import logging
from typing import Callable, Optional, Tuple, Dict, AsyncIterable, Any, Set, Awaitable

import brad.proto_gen.brad_pb2_grpc as brad_grpc
from brad.connection.schema import Schema
from brad.front_end.brad_interface import BradInterface
from brad.front_end.grpc import BradGrpc
from brad.front_end.session import SessionManager, SessionId
from brad.row_list import RowList
from brad.utils.json_decimal_encoder import DecimalEncoder
from brad.vdbe.manager import VdbeFrontEndManager

logger = logging.getLogger(__name__)


# (query_string, vdbe_id, session_id, debug_info) -> (rows, schema)
QueryHandler = Callable[
    [str, int, SessionId, Dict[str, Any]], Awaitable[Tuple[RowList, Optional[Schema]]]
]


class VdbeEndpointManager:
    """
    Used to start/stop VDBE endpoints. Right now, we only support the BRAD gRPC
    interface.
    """

    def __init__(
        self,
        *,
        vdbe_mgr: VdbeFrontEndManager,
        session_mgr: SessionManager,
        handler: QueryHandler,
    ) -> None:
        self._vdbe_mgr = vdbe_mgr
        self._session_mgr = session_mgr
        self._handler = handler
        self._endpoints: Dict[int, Tuple[int, grpc.aio.Server, VdbeGrpcInterface]] = {}

    async def initialize(self) -> None:
        for engine in self._vdbe_mgr.engines():
            endpoint = engine.endpoint
            if endpoint is None:
                logger.warning(
                    "Engine %s (ID: %d) has no endpoint. Skipping adding VDBE endpoint.",
                    engine.internal_id,
                    engine.name,
                )
                continue
            port = int(endpoint.split(":")[1])
            await self.add_vdbe_endpoint(port, engine.internal_id)

    async def shutdown(self) -> None:
        known_ids = list(self._endpoints.keys())
        for vdbe_id in known_ids:
            await self.remove_vdbe_endpoint(vdbe_id)

    async def add_vdbe_endpoint(self, port: int, vdbe_id: int) -> None:
        query_service = VdbeGrpcInterface(
            vdbe_id=vdbe_id, handler=self._handler, session_mgr=self._session_mgr
        )
        grpc_server = grpc.aio.server()
        brad_grpc.add_BradServicer_to_server(BradGrpc(query_service), grpc_server)
        grpc_server.add_insecure_port(f"0.0.0.0:{port}")
        await grpc_server.start()
        logger.info(
            "Added VDBE endpoint for ID %d. Listening on port %d.", vdbe_id, port
        )
        self._endpoints[vdbe_id] = (port, grpc_server, query_service)

    async def remove_vdbe_endpoint(self, vdbe_id: int) -> None:
        try:
            port, grpc_server, query_service = self._endpoints[vdbe_id]
            await query_service.end_all_sessions()
            # See `brad.front_end.BradFrontEnd.serve_forever`.
            grpc_server.__del__()
            del self._endpoints[vdbe_id]
            logger.info("Removed VDBE endpoint for ID %d (was port %d).", vdbe_id, port)

        except KeyError:
            logger.error(
                "Tried to remove VDBE endpoint for ID %d, but it was not found.",
                vdbe_id,
            )


class VdbeGrpcInterface(BradInterface):
    def __init__(
        self, *, vdbe_id: int, handler: QueryHandler, session_mgr: SessionManager
    ) -> None:
        self._vdbe_id = vdbe_id
        self._session_mgr = session_mgr
        self._handler = handler
        self._our_sessions: Set[SessionId] = set()

    async def start_session(self) -> SessionId:
        session_id, _ = await self._session_mgr.create_new_session()
        self._our_sessions.add(session_id)
        return session_id

    def run_query(
        self, session_id: SessionId, query: str, debug_info: Dict[str, Any]
    ) -> AsyncIterable[bytes]:
        # Purposefully not implemented - this is a legacy interface.
        raise NotImplementedError

    async def run_query_json(
        self, session_id: SessionId, query: str, debug_info: Dict[str, Any]
    ) -> str:
        """
        Returns query results encoded as a JSON string.

        This method may throw an error to indicate a problem with the query.
        """
        results, _ = await self._handler(query, self._vdbe_id, session_id, debug_info)
        return json.dumps(results, cls=DecimalEncoder, default=str)

    async def end_session(self, session_id: SessionId) -> None:
        await self._session_mgr.end_session(session_id)
        self._our_sessions.remove(session_id)

    async def end_all_sessions(self) -> None:
        our_sessions = self._our_sessions.copy()
        self._our_sessions.clear()
        for session_id in our_sessions:
            await self._session_mgr.end_session(session_id)
