from brad.planner.workload import Workload


class WorkloadProvider:
    """
    An abstract interface over a component that can provide the next workload
    (for blueprint planning purposes).
    """

    def next_workload(self) -> Workload:
        raise NotImplementedError
