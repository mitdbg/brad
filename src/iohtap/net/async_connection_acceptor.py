import asyncio
import logging
import socket
from typing import Callable, Awaitable

logger = logging.getLogger(__name__)


class AsyncConnectionAcceptor:
    @classmethod
    async def create(
        cls,
        host: str,
        port: int,
        handler_function: Callable[
            [asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]
        ],
    ) -> "AsyncConnectionAcceptor":
        server = await asyncio.start_server(
            handler_function,
            host=host,
            port=port,
            family=socket.AF_INET,
            reuse_address=True,
            start_serving=False,
        )
        return cls(server)

    def __init__(self, server: asyncio.Server):
        self._server = server
        assert len(self._server.sockets) == 1
        for s in self._server.sockets:
            self._host, self._port = s.getsockname()

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    async def serve_forever(self):
        logger.debug(
            "Listening for connections on (%s:%d).",
            self.host,
            self.port,
        )
        await self._server.serve_forever()
