import pathlib
import yaml
from typing import Any, Dict


class TempConfig:
    """
    Stores "temporary" configuration values that BRAD uses.
    """

    @classmethod
    def load_from_file(cls, file_path: str | pathlib.Path) -> "TempConfig":
        with open(file_path, "r", encoding="UTF-8") as file:
            return cls(yaml.load(file, Loader=yaml.Loader))

    def __init__(self, raw: Dict[str, Any]) -> None:
        self._raw = raw

    def latency_ceiling_s(self) -> float:
        return float(self._raw["latency_ceiling_s"])

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