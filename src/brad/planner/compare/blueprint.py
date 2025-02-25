import numpy.typing as npt
from typing import Any, Dict, Optional, List

from brad.config.engine import Engine
from brad.blueprint.provisioning import Provisioning


class ComparableBlueprint:
    """
    An interface representing a `Blueprint` that is used during planning when
    comparing blueprints.
    """

    # Blueprint configuration.

    def get_table_placement(self) -> Dict[str, List[Engine]]:
        raise NotImplementedError

    def get_aurora_provisioning(self) -> Provisioning:
        raise NotImplementedError

    def get_redshift_provisioning(self) -> Provisioning:
        raise NotImplementedError

    def get_routing_decisions(self) -> npt.NDArray:
        """
        The query routing decisions for each analytical query in the workload.
        Use `Workload.EngineLatencyIndex` to map engines to numeric indices.

        Should return an NDArray of shape (N,).
        """
        raise NotImplementedError

    # Predicted performance.

    def get_predicted_analytical_latencies(self) -> npt.NDArray:
        raise NotImplementedError

    def get_predicted_transactional_latencies(self) -> npt.NDArray:
        # Returns (p50, p90) overall latency.
        raise NotImplementedError

    # TODO: For more sophisticated comparisons, the user might want access to
    # predicted latency on a per-query basis.

    def get_operational_monetary_cost(self) -> float:
        raise NotImplementedError

    def get_operational_monetary_cost_without_scans(self) -> float:
        """
        We want to exclude scans because the blueprint uses estimated values. We
        account for scan costs separately (examining the query log).
        """
        raise NotImplementedError

    def get_transition_cost(self) -> float:
        raise NotImplementedError

    def get_transition_time_s(self) -> float:
        raise NotImplementedError

    # Used for efficiency purposes.

    def set_memoized_value(self, key: str, value: Any) -> None:
        raise NotImplementedError

    def get_memoized_value(self, key: str) -> Optional[Any]:
        raise NotImplementedError
