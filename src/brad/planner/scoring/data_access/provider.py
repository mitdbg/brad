from brad.planner.workload import Workload


class DataAccessProvider:
    """
    An abstract interface over a component that attaches data access statistics
    for each query in a workload (for blueprint planning purposes).
    """

    def apply_access_statistics(self, workload: Workload) -> None:
        """
        Decorates the analytical queries in the provided `Workload` with
        data access statistics (number of accessed pages in Aurora, number of
        accessed bytes in Athena).
        """
        raise NotImplementedError
