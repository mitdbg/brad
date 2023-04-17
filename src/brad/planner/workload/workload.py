from typing import List

from .query_template import QueryTemplate


class Workload:
    """
    A representation of the workload to be considered during the blueprint
    planning process.
    """

    def __init__(self, templates: List[QueryTemplate]) -> None:
        self._templates = templates

    def templates(self) -> List[QueryTemplate]:
        return self._templates
