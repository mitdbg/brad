import yaml
from typing import Dict


class PlannerConfig:
    """
    Constants used by the neighborhood-based blueprint planner.
    """

    def __init__(self, path: str):
        self._raw_path = path
        with open(path, "r", encoding="UTF-8") as file:
            self._raw = yaml.load(file, Loader=yaml.Loader)

    def max_num_table_moves(self) -> int:
        return int(self._raw["max_num_table_moves"])

    def max_provisioning_multiplier(self) -> float:
        return float(self._raw["max_provisioning_multiplier"])

    def athena_usd_per_mb_scanned(self) -> float:
        return float(self._raw["athena_usd_per_mb_scanned"])

    def aurora_usd_per_mb_scanned(self) -> float:
        return float(self._raw["aurora_usd_per_mb_scanned"])

    def redshift_provisioning_change_time_s(self) -> int:
        return int(self._raw["redshift_provisioning_change_time_s"])

    def aurora_provisioning_change_time_s(self) -> int:
        return int(self._raw["aurora_provisioning_change_time_s"])

    def redshift_extract_rate_mb_per_s(self) -> float:
        return float(self._raw["redshift_extract_rate_mb_per_s"])

    def redshift_load_rate_mb_per_s(self) -> float:
        return float(self._raw["redshift_load_rate_mb_per_s"])

    def aurora_extract_rate_mb_per_s(self) -> float:
        return float(self._raw["aurora_extract_rate_mb_per_s"])

    def aurora_load_rate_mb_per_s(self) -> float:
        return float(self._raw["aurora_load_rate_mb_per_s"])

    def athena_extract_rate_mb_per_s(self) -> float:
        return float(self._raw["athena_extract_rate_mb_per_s"])

    def athena_load_rate_mb_per_s(self) -> float:
        return float(self._raw["athena_load_rate_mb_per_s"])

    def dataset_scaling_modifiers(self) -> Dict[str, float]:
        return self._raw["dataset_scaling"]

    def redshift_resource_scaling_modifiers(self) -> Dict[str, float]:
        return self._raw["redshift_resource_scaling"]

    def aurora_resource_scaling_modifiers(self) -> Dict[str, float]:
        return self._raw["aurora_resource_scaling"]
