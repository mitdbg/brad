import sys
from collections import deque
from typing import List, Iterator

from brad.data_sync.operators import Operator


class PhysicalDataSyncPlan:
    def __init__(self, base_ops: List[Operator], all_operators: List[Operator]) -> None:
        # All operators that have no dependencies. This list is a subset of
        # `all_operators`.
        self._base_ops = base_ops
        self._all_operators = all_operators

    def base_ops(self) -> List[Operator]:
        return self._base_ops

    def all_operators(self) -> List[Operator]:
        return self._all_operators

    def print_plan_sequentially(self, file=sys.stdout) -> None:
        """
        Prints a topological ordering of the plan. Useful for debugging
        purposes.
        """
        for str_op in self.traverse_plan_sequentially():
            print(str_op, file=file)

    def traverse_plan_sequentially(self) -> Iterator[str]:
        """
        Yields a string-based topological ordering of the plan. Useful for
        debugging purposes.
        """
        for op in self._all_operators:
            op.reset_ready_to_run()

        ready_to_process = deque([*self._base_ops])
        while len(ready_to_process) > 0:
            op = ready_to_process.popleft()
            yield "- {}".format(str(op))
            for dependee in op.dependees():
                dependee.mark_dependency_complete()
                if dependee.ready_to_run():
                    ready_to_process.append(dependee)
