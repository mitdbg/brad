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
