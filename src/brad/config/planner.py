import math
import yaml
import logging
import numpy as np
import numpy.typing as npt
import importlib.resources as pkg_resources
from datetime import timedelta
from typing import Dict, Optional, Any, Tuple
from brad.planner.strategy import PlanningStrategy
import brad.planner as brad_planner

logger = logging.getLogger(__name__)


class PlannerConfig:
    """
    Configuration constants used by the blueprint planners. Some constants are
    shared across planning strategies, some are specific to a strategy.
    """

    @classmethod
    def load_from_new_configs(cls, system_config: str) -> "PlannerConfig":
        with open(system_config, "r", encoding="UTF-8") as file:
            system_config_dict = yaml.load(file, Loader=yaml.Loader)
        with pkg_resources.files(brad_planner).joinpath("constants.yml").open(
            "r"
        ) as data:
            system_constants_dict = yaml.load(data, Loader=yaml.Loader)

        merged = {}
        merged.update(system_config_dict)
        merged.update(system_constants_dict)
        return cls(merged)

    @classmethod
    def load_only_constants(cls) -> "PlannerConfig":
        with pkg_resources.files(brad_planner).joinpath("constants.yml").open(
            "r"
        ) as data:
            system_constants_dict = yaml.load(data, Loader=yaml.Loader)
        return cls(system_constants_dict)

    @classmethod
    def load(cls, path: str) -> "PlannerConfig":
        with open(path, "r", encoding="UTF-8") as file:
            raw = yaml.load(file, Loader=yaml.Loader)
        return cls(raw)

    def __init__(self, raw: Dict[str, Any]):
        self._raw = raw

        self._aurora_new_scaling_coefs: Optional[npt.NDArray] = None
        self._redshift_new_scaling_coefs: Optional[npt.NDArray] = None

        # Deprecated
        self._aurora_scaling_coefs: Optional[npt.NDArray] = None
        self._redshift_scaling_coefs: Optional[npt.NDArray] = None

    def strategy(self) -> PlanningStrategy:
        return PlanningStrategy.from_str(self._raw["strategy"])

    def planning_window(self) -> timedelta:
        epoch = self._raw["planning_window"]
        return timedelta(
            weeks=epoch["weeks"],
            days=epoch["days"],
            hours=epoch["hours"],
            minutes=epoch["minutes"],
        )

    def trigger_configs(self) -> Dict[str, Any]:
        return self._raw["triggers"]

    def triggers_enabled(self) -> bool:
        return self._raw["triggers"]["enabled"]

    def query_dist_change_frac(self) -> float:
        return float(self._raw["query_dist_change_frac"])

    def reinterpret_second_as(self) -> Optional[timedelta]:
        if "reinterpret_second_as" not in self._raw:
            return None
        return timedelta(seconds=int(self._raw["reinterpret_second_as"]))

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

    def aurora_per_instance_change_time_s(self) -> int:
        return int(self._raw["aurora_per_instance_change_time_s"])

    def redshift_elastic_resize_time_s(self) -> int:
        return int(self._raw["redshift_elastic_resize_time_s"])

    def redshift_classic_resize_time_s(self) -> int:
        # We may replace this with our own resize operation.
        return int(self._raw["redshift_classic_resize_time_s"])

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

    def aurora_regular_usd_per_mb_per_month(self) -> float:
        return float(self._raw["aurora_regular_usd_per_mb_per_month"])

    def aurora_io_opt_usd_per_mb_per_month(self) -> float:
        return float(self._raw["aurora_io_opt_usd_per_mb_per_month"])

    def sample_set_size(self) -> int:
        return int(self._raw["sample_set_size"])

    ###
    ### Provisioning scaling
    ###
    def aurora_alpha(self) -> float:
        return float(self._raw["aurora_alpha"])

    def aurora_gamma(self) -> float:
        return float(self._raw["aurora_gamma"])

    def redshift_alpha(self) -> float:
        return float(self._raw["redshift_alpha"])

    def redshift_gamma(self) -> float:
        return float(self._raw["redshift_gamma"])

    ###
    ### Redshift load scaling
    ###
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

    ###
    ### Aurora load scaling
    ###
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

    ###
    ### Extraction: Bytes per row
    ###
    def extract_table_bytes_per_row(self, schema_name: str, table_name: str) -> float:
        return float(self._raw["table_extract_bytes_per_row"][schema_name][table_name])

    ###
    ### Transactions
    ###
    def client_txn_to_load(self) -> float:
        return self._raw["aurora_txns"]["client_thpt_to_load"]

    def client_txn_to_cpu_denorm(self) -> float:
        return self._raw["aurora_txns"]["client_thpt_to_cpu_denorm"]

    def aurora_prov_to_peak_cpu_denorm(self) -> float:
        return self._raw["aurora_txns"]["prov_to_peak_cpu_denorm"]

    ###
    ### Unified Aurora scaling
    ###
    def aurora_scaling_coefs(self) -> npt.NDArray:
        if self._aurora_scaling_coefs is None:
            coefs = self._raw["aurora_scaling"]
            self._aurora_scaling_coefs = np.array(
                [coefs["coef1"], coefs["coef2"], coefs["coef3"], coefs["coef4"]]
            )
        return self._aurora_scaling_coefs

    def aurora_txn_coefs(self, schema_name: str) -> Dict[str, float]:
        return self._raw["aurora_txns"][schema_name]

    def aurora_new_scaling_coefs(self) -> npt.NDArray:
        if self._aurora_new_scaling_coefs is None:
            coefs = self._raw["aurora_scaling_new"]
            self._aurora_new_scaling_coefs = np.array([coefs["coef1"], coefs["coef2"]])
        return self._aurora_new_scaling_coefs

    def aurora_new_scaling_alpha(self) -> float:
        return self._raw["aurora_scaling_new"]["alpha"]

    ###
    ### Unified Redshift scaling
    ###
    def redshift_scaling_coefs(self) -> npt.NDArray:
        if self._redshift_scaling_coefs is None:
            coefs = self._raw["redshift_scaling"]
            self._redshift_scaling_coefs = np.array(
                [coefs["coef1"], coefs["coef2"], coefs["coef3"], coefs["coef4"]]
            )
        return self._redshift_scaling_coefs

    def redshift_new_scaling_coefs(self) -> npt.NDArray:
        if self._redshift_new_scaling_coefs is None:
            coefs = self._raw["redshift_scaling_new"]
            self._redshift_new_scaling_coefs = np.array(
                [coefs["coef1"], coefs["coef2"]]
            )
        return self._redshift_new_scaling_coefs

    def redshift_new_scaling_alpha(self) -> float:
        return self._raw["redshift_scaling_new"]["alpha"]

    def use_io_optimized_aurora(self) -> bool:
        if "use_io_optimized_aurora" not in self._raw:
            # By default.
            return True
        else:
            return self._raw["use_io_optimized_aurora"]

    def flag(self, key: str, default: bool = False) -> bool:
        if key not in self._raw:
            return default
        else:
            return self._raw[key]

    def aurora_initialize_load_fraction(self) -> float:
        return self._raw["aurora_initialize_load_fraction"]

    def redshift_initialize_load_fraction(self) -> float:
        return self._raw["redshift_initialize_load_fraction"]

    def aurora_storage_index_multiplier(self) -> float:
        return float(self._raw["aurora_storage_index_multiplier"])

    def metrics_agg(self) -> Dict[str, Any]:
        return self._raw["metrics_agg"]

    def aurora_min_load_removal_fraction(self) -> float:
        try:
            return self._raw["aurora_min_load_removal_fraction"]
        except KeyError:
            logger.warning("Using default Aurora min load removal fraction: 0.75")
            return 0.75

    def redshift_min_load_removal_fraction(self) -> float:
        try:
            return self._raw["redshift_min_load_removal_fraction"]
        except KeyError:
            logger.warning("Using default Redshift min load removal fraction: 0.75")
            return 0.75

    def aurora_max_query_factor(self) -> Tuple[float, float]:
        try:
            return (
                self._raw["aurora_max_query_factor"],
                self._raw["aurora_max_query_factor_replace"],
            )
        except KeyError:
            return math.inf, math.inf

    def redshift_peak_load_multiplier(self) -> Tuple[float, float]:
        try:
            return (
                self._raw["redshift_peak_load_threshold"],
                self._raw["redshift_peak_load_multiplier"],
            )
        except KeyError:
            return 110.0, 1.0
