from collections import deque
from typing import List

from brad.data_sync.operators import Operator


class PhysicalDataSyncPlan:
    def __init__(self, start_op: Operator, all_operators: List[Operator]) -> None:
        self._start_op = start_op
        self._all_operators = all_operators

    def start_op(self) -> Operator:
        return self._start_op

    def all_operators(self) -> List[Operator]:
        return self._all_operators

    def print_sequential_plan(self) -> None:
        """
        Prints a topological ordering of the plan. Useful for debugging
        purposes.
        """
        for op in self._all_operators:
            op.reset_ready_to_run()

        print("Physical Data Sync Plan:")
        ready_to_process = deque([self._start_op])
        while len(ready_to_process) > 0:
            op = ready_to_process.popleft()
            print("-", str(op))
            for dependee in op.dependees():
                dependee.mark_dependency_complete()
                if dependee.ready_to_run():
                    ready_to_process.append(dependee)
