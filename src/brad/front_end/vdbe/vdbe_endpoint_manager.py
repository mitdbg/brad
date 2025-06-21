import asyncio
import grpc
import json
import logging
import threading
from typing import (
    Callable,
    Optional,
    Tuple,
    Dict,
    AsyncIterable,
    Any,
    Set,
    Awaitable,
)

import brad.proto_gen.brad_pb2_grpc as brad_grpc
from brad.config.file import ConfigFile
from brad.connection.schema import Schema, DataType
from brad.front_end.brad_interface import BradInterface
from brad.front_end.grpc import BradGrpc
from brad.front_end.session import SessionManager, SessionId
from brad.row_list import RowList
from brad.utils.json_decimal_encoder import DecimalEncoder
from brad.vdbe.manager import VdbeFrontEndManager

logger = logging.getLogger(__name__)


# (query_string, vdbe_id, session_id, debug_info, retrieve_schema) -> (rows, schema)
QueryHandler = Callable[
    [str, int, SessionId, Dict[str, Any], bool],
    Awaitable[Tuple[RowList, Optional[Schema]]],
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
        config: ConfigFile,
    ) -> None:
        self._vdbe_mgr = vdbe_mgr
        self._session_mgr = session_mgr
        self._handler = handler
        self._config = config
        self._endpoints: Dict[int, Tuple[int, grpc.aio.Server, VdbeGrpcInterface]] = {}
        self._flight_sql_endpoints: Dict[int, Tuple[int, VdbeFlightSqlServer]] = {}

        try:
            # pylint: disable-next=import-error,no-name-in-module,unused-import
            import brad.native.pybind_brad_server as brad_server

            self._use_flight_sql = self._config.flight_sql_mode() == "vdbe"
        except ImportError:
            self._use_flight_sql = False

        if self._use_flight_sql:
            logger.info("Will start Flight SQL endpoints for VDBEs.")
        else:
            logger.info(
                "Flight SQL endpoints for VDBEs are not available. "
                "Using gRPC endpoints only."
            )

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
            "Added gRPC VDBE endpoint for ID %d. Listening on port %d.", vdbe_id, port
        )
        self._endpoints[vdbe_id] = (port, grpc_server, query_service)

        if self._use_flight_sql:
            session_id, _ = await self._session_mgr.create_new_session()
            # The flight SQL port is offset by 10,000 from the gRPC port.
            flight_sql_port = port + 10_000
            flight_sql_server = VdbeFlightSqlServer(
                vdbe_id=vdbe_id,
                port=flight_sql_port,
                main_loop=asyncio.get_event_loop(),
                handler=self._handler,
                session_id=session_id,
            )
            flight_sql_server.start()
            self._flight_sql_endpoints[vdbe_id] = (flight_sql_port, flight_sql_server)
            logger.info(
                "Added Flight SQL VDBE endpoint for ID %d. Listening on port %d.",
                vdbe_id,
                flight_sql_port,
            )

    async def remove_vdbe_endpoint(self, vdbe_id: int) -> None:
        try:
            port, grpc_server, query_service = self._endpoints[vdbe_id]
            await query_service.end_all_sessions()
            # See `brad.front_end.BradFrontEnd.serve_forever`.
            grpc_server.__del__()
            del self._endpoints[vdbe_id]
            logger.info(
                "Removed gRPC VDBE endpoint for ID %d (was port %d).", vdbe_id, port
            )

        except KeyError:
            logger.error(
                "Tried to remove gRPC VDBE endpoint for ID %d, but it was not found.",
                vdbe_id,
            )

        try:
            port, flight_sql_server = self._flight_sql_endpoints[vdbe_id]
            flight_sql_server.stop()
            del self._flight_sql_endpoints[vdbe_id]
            await self._session_mgr.end_session(flight_sql_server.session_id)
            logger.info(
                "Removed Flight SQL VDBE endpoint for ID %d (was port %d).",
                vdbe_id,
                port,
            )
        except KeyError:
            logger.error(
                "Tried to remove Flight SQL VDBE endpoint for ID %d, but it was not found.",
                vdbe_id,
            )

    async def reconcile(self) -> Tuple[int, int]:
        to_add = []
        to_remove = []
        seen_ids = set()

        for engine in self._vdbe_mgr.engines():
            if engine.internal_id not in self._endpoints:
                to_add.append(engine)
            seen_ids.add(engine.internal_id)

        for vdbe_id in self._endpoints.keys():
            if vdbe_id not in seen_ids:
                to_remove.append(vdbe_id)

        for vdbe in to_add:
            if vdbe.endpoint is None:
                logger.warning(
                    "VDBE %s (ID: %d) has no endpoint. Skipping adding VDBE endpoint.",
                    vdbe.name,
                    vdbe.internal_id,
                )
                continue
            port = int(vdbe.endpoint.split(":")[1])
            await self.add_vdbe_endpoint(port, vdbe.internal_id)

        for vdbe_id in to_remove:
            await self.remove_vdbe_endpoint(vdbe_id)

        return len(to_add), len(to_remove)


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
        results, _ = await self._handler(
            query, self._vdbe_id, session_id, debug_info, False
        )
        return json.dumps(results, cls=DecimalEncoder, default=str)

    async def end_session(self, session_id: SessionId) -> None:
        await self._session_mgr.end_session(session_id)
        self._our_sessions.remove(session_id)

    async def end_all_sessions(self) -> None:
        our_sessions = self._our_sessions.copy()
        self._our_sessions.clear()
        for session_id in our_sessions:
            await self._session_mgr.end_session(session_id)


class VdbeFlightSqlServer:
    def __init__(
        self,
        *,
        vdbe_id: int,
        port: int,
        main_loop: asyncio.AbstractEventLoop,
        handler: QueryHandler,
        session_id: SessionId,
    ) -> None:
        # pylint: disable-next=import-error,no-name-in-module
        import brad.native.pybind_brad_server as brad_server

        # pylint: disable-next=c-extension-no-member
        self._flight_sql_server = brad_server.BradFlightSqlServer()
        self._flight_sql_server.init("0.0.0.0", port, self._handle_query)
        self._thread = threading.Thread(
            name=f"FlightSqlServer-{vdbe_id}", target=self._serve
        )
        self._vdbe_id = vdbe_id
        self._port = port
        # Important: The endpoint manager is responsible for creating and
        # terminating the session.
        self.session_id = session_id

        self._main_loop = main_loop
        self._handler = handler

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        logger.info(
            "BRAD FlightSQL server stopping (port %d, VDBE %d)...",
            self._port,
            self._vdbe_id,
        )
        self._flight_sql_server.shutdown()
        self._thread.join()
        logger.info(
            "BRAD FlightSQL server stopped (port %d, VDBE %d).",
            self._port,
            self._vdbe_id,
        )

    def _serve(self) -> None:
        self._flight_sql_server.serve()

    def _handle_query(self, query: str) -> Tuple[RowList, Schema]:
        # This method is called from a separate thread. So it's very important
        # to schedule the handler on the main event loop thread.
        debug_info: Dict[str, Any] = {}
        future = asyncio.run_coroutine_threadsafe(  # type: ignore
            self._handler(query, self._vdbe_id, self.session_id, debug_info, True),  # type: ignore
            self._main_loop,
        )
        row_result, schema = future.result()
        assert schema is not None

        # We need to do extra processing for decimal fields since our C++
        # backend expects them as strings.
        decimal_fields = []
        for idx, field in enumerate(schema.fields):
            if field.data_type == DataType.Decimal:
                decimal_fields.append(idx)

        if len(decimal_fields) > 0:
            new_rows = []
            for row in row_result:
                new_row = tuple(
                    str(value) if idx in decimal_fields else value
                    for idx, value in enumerate(row)
                )
                new_rows.append(new_row)
            row_result = new_rows

        return row_result, schema
