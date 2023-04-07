import sys
from collections import deque
from typing import List

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
        for op in self._all_operators:
            op.reset_ready_to_run()

        print("Physical Data Sync Plan:", file=file)
        ready_to_process = deque([*self._base_ops])
        while len(ready_to_process) > 0:
            op = ready_to_process.popleft()
            print("-", str(op), file=file)
            for dependee in op.dependees():
                dependee.mark_dependency_complete()
                if dependee.ready_to_run():
                    ready_to_process.append(dependee)
