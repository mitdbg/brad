from brad.planner.workload import Workload


class WorkloadProvider:
    """
    An abstract interface over a component that can provide the next workload
    (for blueprint planning purposes).
    """

    def next_workload(self) -> Workload:
        raise NotImplementedError


class FixedWorkloadProvider(WorkloadProvider):
    """
    Always returns the same workload. Used for debugging purposes.
    """

    def __init__(self, workload: Workload) -> None:
        self._workload = workload

    def next_workload(self) -> Workload:
        return self._workload
