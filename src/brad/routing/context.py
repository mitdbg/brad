from typing import Optional

from brad.planner.estimator import Estimator


class RoutingContext:
    """
    A wrapper class that holds state that should be used for routing.
    """

    def __init__(self) -> None:
        self.estimator: Optional[Estimator] = None
