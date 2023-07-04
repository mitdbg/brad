import yaml
from typing import Dict
from brad.planner.strategy import PlanningStrategy


class PlannerConfig:
    """
    Configuration constants used by the blueprint planners. Some constants are
    shared across planning strategies, some are specific to a strategy.
    """

    def __init__(self, path: str):
        self._raw_path = path
        with open(path, "r", encoding="UTF-8") as file:
            self._raw = yaml.load(file, Loader=yaml.Loader)

    def strategy(self) -> PlanningStrategy:
        return PlanningStrategy.from_str(self._raw["strategy"])

    def beam_size(self) -> int:
        return int(self._raw["beam_size"])

    def max_num_table_moves(self) -> int:
        return int(self._raw["max_num_table_moves"])

    def max_provisioning_multiplier(self) -> float:
        return float(self._raw["max_provisioning_multiplier"])

    def athena_usd_per_mb_scanned(self) -> float:
        return float(self._raw["athena_usd_per_mb_scanned"])

    def athena_min_mb_per_query(self) -> int:
        return int(self._raw["athena_min_mb_per_query"])

    def aurora_usd_per_million_ios(self) -> float:
        return float(self._raw["aurora_usd_per_million_ios"])

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

    def s3_usd_per_mb_per_month(self) -> float:
        return float(self._raw["s3_usd_per_mb_per_month"])

    def sample_set_size(self) -> int:
        return int(self._raw["sample_set_size"])

    def aurora_alpha(self) -> float:
        return float(self._raw["aurora_alpha"])

    def aurora_gamma(self) -> float:
        return float(self._raw["aurora_gamma"])

    def redshift_alpha(self) -> float:
        return float(self._raw["redshift_alpha"])

    def redshift_gamma(self) -> float:
        return float(self._raw["redshift_gamma"])

    # Redshift load scaling
    def redshift_load_resource_alpha(self) -> float:
        return float(self._raw["redshift_load_factor"]["resource_alpha"])

    def redshift_load_cpu_alpha(self) -> float:
        return float(self._raw["redshift_load_factor"]["cpu_alpha"])

    def redshift_load_cpu_gamma(self) -> float:
        return float(self._raw["redshift_load_factor"]["cpu_gamma"])

    # These two are legacy scaling factors.
    def redshift_load_cpu_to_load_alpha(self) -> float:
        return float(self._raw["redshift_load_factor"]["cpu_to_load_alpha"])

    def redshift_load_min_scaling_cpu(self) -> float:
        return float(self._raw["redshift_load_factor"]["min_scaling_cpu"])

    # Aurora load scaling
    def aurora_load_resource_alpha(self) -> float:
        return float(self._raw["aurora_load_factor"]["resource_alpha"])

    def aurora_load_alpha(self) -> float:
        return float(self._raw["aurora_load_factor"]["load_alpha"])

    # These two are legacy scaling factors.
    def aurora_load_cpu_to_load_alpha(self) -> float:
        return float(self._raw["aurora_load_factor"]["cpu_to_load_alpha"])

    def aurora_load_min_scaling_cpu(self) -> float:
        return float(self._raw["aurora_load_factor"]["min_scaling_cpu"])

    def max_feasible_cpu(self) -> float:
        return float(self._raw["max_feasible_cpu"])

    # Extraction: Bytes per row
    def extract_table_bytes_per_row(self, schema_name: str, table_name: str) -> float:
        return float(self._raw["table_extract_bytes_per_row"][schema_name][table_name])
