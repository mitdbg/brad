from typing import List, Iterable

from brad.data_sync.execution.context import ExecutionContext


class Operator:
    def __init__(self) -> None:
        self._dependencies: List[Operator] = []
        self._dependees: List[Operator] = []
        self._num_pending_dependencies = 0

    def dependees(self) -> List["Operator"]:
        """
        Other `Operator`s that depend on this `Operator`.

        These `Operator`s cannot necessarily run until this operator has
        successfully completed.
        """
        return self._dependees

    def dependencies(self) -> List["Operator"]:
        """
        Other `Operator`s that this `Operator` depends on.

        This operator cannot be executed until these `Operator`s have all
        successfully completed.
        """
        return self._dependencies

    def add_dependency(self, other: "Operator") -> None:
        self._dependencies.append(other)
        other._dependees.append(self)  # pylint: disable=protected-access

    def add_dependencies(self, others: Iterable["Operator"]) -> None:
        for op in others:
            self.add_dependency(op)

    def reset_ready_to_run(self) -> None:
        self._num_pending_dependencies = len(self._dependencies)

    def ready_to_run(self) -> bool:
        return self._num_pending_dependencies == len(self._dependencies)

    def mark_dependency_complete(self) -> None:
        self._num_pending_dependencies -= 1

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        raise NotImplementedError
