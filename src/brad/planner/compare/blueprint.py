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

    def table_placement(self) -> Dict[str, List[Engine]]:
        raise NotImplementedError

    def aurora_provisioning(self) -> Provisioning:
        raise NotImplementedError

    def redshift_provisioning(self) -> Provisioning:
        raise NotImplementedError

    # Predicted performance.

    def predicted_analytical_latencies(self) -> npt.NDArray:
        raise NotImplementedError

    # TODO: For more sophisticated comparisons, the user might want access to
    # predicted latency on a per-query basis.

    def operational_monetary_cost(self) -> float:
        raise NotImplementedError

    def transition_cost(self) -> float:
        raise NotImplementedError

    def transition_time_s(self) -> float:
        raise NotImplementedError

    # Used for efficiency purposes.

    def set_memoized_value(self, key: str, value: Any) -> None:
        raise NotImplementedError

    def get_memoized_value(self, key: str) -> Optional[Any]:
        raise NotImplementedError
