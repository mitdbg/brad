import asyncio
from typing import Coroutine, Callable, List

from brad.blueprint import Blueprint

NewBlueprintCallback = Callable[[Blueprint], Coroutine[None, None, None]]


class BlueprintPlanner:
    """
    `BlueprintPlanner`s should not run in the same process as the BRAD server.
    The optimization process will be long running and Python's GIL prevents
    multiple Python threads from executing in parallel.
    """

    def __init__(self) -> None:
        self._callbacks: List[NewBlueprintCallback] = []

    async def run_forever(self) -> None:
        """
        Called to start the planner. The planner is meant to run until its task
        is cancelled.
        """
        raise NotImplementedError

    # NOTE: In the future we will implement an abstraction that will allow for a
    # generic planner to subscribe to a stream of events, used to detect when to
    # trigger re-planning.

    def register_new_blueprint_callback(self, callback: NewBlueprintCallback) -> None:
        """
        Register a function to be called when this planner selects a new
        `Blueprint`.
        """
        self._callbacks.append(callback)

    async def _notify_new_blueprint(self, blueprint: Blueprint) -> None:
        """
        Concrete planners should call this method to notify subscribers about
        the next blueprint.
        """
        tasks = []
        for callback in self._callbacks:
            tasks.append(asyncio.create_task(callback(blueprint)))
        await asyncio.gather(*tasks)
