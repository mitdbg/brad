import pathlib
import yaml
from typing import Any, Dict, Optional, List
from datetime import timedelta


class TempConfig:
    """
    Stores "temporary" configuration values that BRAD uses.
    """

    @classmethod
    def load_from_new_configs(cls, system_config: str) -> "TempConfig":
        with open(system_config, "r", encoding="UTF-8") as file:
            return cls(yaml.load(file, Loader=yaml.Loader))

    @classmethod
    def load_from_file(cls, file_path: str | pathlib.Path) -> "TempConfig":
        with open(file_path, "r", encoding="UTF-8") as file:
            return cls(yaml.load(file, Loader=yaml.Loader))

    def __init__(self, raw: Dict[str, Any]) -> None:
        self._raw = raw

    def query_latency_p90_ceiling_s(self) -> float:
        return float(self._raw["query_latency_p90_ceiling_s"])

    def txn_latency_p50_ceiling_s(self) -> float:
        return float(self._raw["txn_latency_p50_ceiling_s"])

    def txn_latency_p90_ceiling_s(self) -> float:
        return float(self._raw["txn_latency_p90_ceiling_s"])

    def comparator_type(self) -> str:
        return self._raw["comparator"]["type"]

    def benefit_horizon(self) -> timedelta:
        period = self._raw["comparator"]["benefit_horizon"]
        return timedelta(
            weeks=period["weeks"],
            days=period["days"],
            hours=period["hours"],
            minutes=period["minutes"],
        )

    def penalty_threshold(self) -> float:
        return self._raw["comparator"]["penalty_threshold"]

    def penalty_power(self) -> float:
        comparator_config = self._raw["comparator"]
        if "penalty_power" in comparator_config:
            return comparator_config["penalty_power"]
        else:
            return 1.0

    def std_datasets(self) -> List[Dict[str, str]]:
        if "std_datasets" not in self._raw:
            return []
        return self._raw["std_datasets"]

    # The below configs are now deprecated.

    def std_dataset_path(self) -> Optional[pathlib.Path]:
        if "std_dataset_path" not in self._raw:
            return None
        return pathlib.Path(self._raw["std_dataset_path"])

    def aurora_preds_path(self) -> pathlib.Path:
        return pathlib.Path(self._raw["aurora_preds_path"])

    def athena_preds_path(self) -> pathlib.Path:
        return pathlib.Path(self._raw["athena_preds_path"])

    def redshift_preds_path(self) -> pathlib.Path:
        return pathlib.Path(self._raw["redshift_preds_path"])

    def aurora_data_access_path(self) -> pathlib.Path:
        return pathlib.Path(self._raw["aurora_data_access_path"])

    def athena_data_access_path(self) -> pathlib.Path:
        return pathlib.Path(self._raw["athena_data_access_path"])

    def query_bank_path(self) -> pathlib.Path:
        return pathlib.Path(self._raw["query_bank_path"])
