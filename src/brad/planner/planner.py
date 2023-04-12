from typing import Callable
from brad.blueprint import Blueprint


class BlueprintPlanner:
    """
    `BlueprintPlanner`s should be launched in a background process since the
    optimization process will be long running and Python's GIL prevents multiple
    Python threads from executing in parallel.
    """

    def register_new_blueprint_callback(
        self, callback: Callable[[Blueprint], None]
    ) -> None:
        """
        Register a function to be called when this planner selects a new
        `Blueprint`.
        """
        raise NotImplementedError

    # NOTE: In the future we will implement an abstraction that will allow for a
    # generic planner to subscribe to a stream of events, used to detect when to
    # trigger re-planning.

    def start_running(self) -> None:
        """
        Starts executing this planner. The `BlueprintPlanner` should run on its
        own thread (i.e., it should not run on the calling thread).
        """
        raise NotImplementedError

    def shutdown(self) -> None:
        """
        Shut down this planner. This method should block until the planner has
        finished shutting down.
        """
        raise NotImplementedError
