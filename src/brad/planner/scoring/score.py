import pickle
import numpy.typing as npt
from typing import Dict, Optional, TYPE_CHECKING

from brad.config.engine import Engine

if TYPE_CHECKING:
    from .performance.unified_aurora import AuroraProvisioningScore
    from .performance.unified_redshift import RedshiftProvisioningScore


class Score:
    """
    Collects all of a blueprint's scoring components. This is used when we want
    to persist the score beyond the planner.
    """

    def __init__(self) -> None:
        self.provisioning_cost = 0.0
        self.storage_cost = 0.0
        self.table_movement_trans_cost = 0.0

        self.workload_scan_cost = 0.0
        self.athena_scanned_bytes = 0
        self.aurora_accessed_pages = 0

        self.table_movement_trans_time_s = 0.0
        self.provisioning_trans_time_s = 0.0

        self.scaled_query_latencies: Dict[Engine, npt.NDArray] = {}
        self.aurora_score: Optional["AuroraProvisioningScore"] = None
        self.redshift_score: Optional["RedshiftProvisioningScore"] = None

        self.aurora_queries = 0
        self.athena_queries = 0
        self.redshift_queries = 0

    def serialize(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, raw: bytes) -> "Score":
        return pickle.loads(raw)
