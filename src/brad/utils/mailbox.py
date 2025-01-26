import asyncio
from typing import Awaitable, Callable, Generic, TypeVar, Optional

S = TypeVar("S")
R = TypeVar("R")


class Mailbox(Generic[S, R]):
    """
    A utility class used to manage asynchronous request-response use cases.
    """

    def __init__(self, do_send_msg: Callable[[S], Awaitable[None]]) -> None:
        self._do_send_msg = do_send_msg
        self._event = asyncio.Event()
        self._inbox: Optional[R] = None
        self._is_active = False

    def is_active(self) -> bool:
        return self._is_active

    async def send_recv(self, msg: S) -> R:
        try:
            self._is_active = True
            await self._send(msg)
            response = await self._recv()
            self._inbox = None
            return response
        finally:
            self._is_active = False

    async def _send(self, msg: S) -> None:
        self._event.clear()
        await self._do_send_msg(msg)

    async def _recv(self) -> R:
        await self._event.wait()
        assert self._inbox is not None
        return self._inbox

    def on_new_message(self, msg: R) -> None:
        self._inbox = msg
        self._event.set()
