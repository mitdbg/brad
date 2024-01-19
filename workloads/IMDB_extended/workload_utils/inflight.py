import asyncio
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from typing import Callable, TypeVar, Generic, Deque, List, Set, Tuple

Context = TypeVar("Context")
Result = TypeVar("Result")


class InflightHelper(Generic[Context, Result]):
    """
    Helper class used to support multiple inflight queries.
    """

    def __init__(
        self, contexts: List[Context], on_result: Callable[[Result], None]
    ) -> None:
        self._free_slots: Deque[Context] = deque(contexts)
        self._executor = ThreadPoolExecutor(max_workers=len(contexts))
        self._loop = asyncio.get_running_loop()
        self._inflight: Set[asyncio.Future] = set()
        self._on_result = on_result

    def submit(self, runnable: Callable[[Context], Result]) -> bool:
        """
        Returns `True` if this runnable was scheduled successfully. If `False`
        was returned, it means all slots are currently supporting in-flight
        code.
        """
        if len(self._free_slots) == 0:
            return False

        ctx = self._free_slots.popleft()
        future = asyncio.ensure_future(
            self._loop.run_in_executor(
                self._executor, self._run_with_context, ctx, runnable
            )
        )
        self._inflight.add(future)
        return True

    async def wait_for_s(self, duration_s: float) -> None:
        time_future = asyncio.ensure_future(asyncio.sleep(duration_s))
        pending = self._inflight.copy()
        pending.add(time_future)
        timer_expired = False
        while not timer_expired:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for fut in done:
                result = fut.result()
                if result is None:
                    # This is the timer task.
                    timer_expired = True
                    continue
                self._handle_result(result)
        self._inflight = pending

    async def wait_until_complete(self) -> None:
        pending = self._inflight.copy()
        while len(pending) > 0:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for fut in done:
                result = fut.result()
                self._handle_result(result)
        self._inflight = pending

    def _run_with_context(
        self, context: Context, runnable: Callable[[Context], Result]
    ) -> Tuple[Context, Result]:
        result = runnable(context)
        return context, result

    def _handle_result(self, result: Tuple[Context, Result]) -> None:
        context, res = result
        self._free_slots.append(context)
        self._on_result(res)
