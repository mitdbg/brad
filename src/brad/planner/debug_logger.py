import csv
import datetime
import pathlib
import pytz
import pickle
from typing import Optional, Dict, List, Any

from brad.config.file import ConfigFile


class BlueprintPlanningDebugLogger:
    """
    Used for debugging purposes.
    """

    @classmethod
    def create_if_requested(
        cls, config: ConfigFile, file_name_prefix: str
    ) -> Optional["BlueprintPlanningDebugLogger"]:
        log_path = config.planner_log_path
        if log_path is None:
            return None
        return cls(log_path, file_name_prefix)

    def __init__(self, log_path: pathlib.Path, file_name_prefix: str) -> None:
        out_file_name = f"{file_name_prefix}_{_get_timestamp_str()}.csv"
        self._out_file = open(log_path / out_file_name, "w", encoding="UTF-8")
        self._key_order: List[str] = []
        self._first_log = True

    def __del__(self) -> None:
        self._out_file.close()

    def log_debug_values(self, values: Dict[str, int | float | str]) -> None:
        writer = csv.writer(self._out_file)

        if self._first_log:
            self._first_log = False
            self._key_order = list(values.keys())
            writer.writerow(self._key_order)

        row: List[int | float | str] = []
        for key in self._key_order:
            if key not in values:
                row.append("")
            else:
                row.append(values[key])
        writer.writerow(row)
        self._out_file.flush()


class BlueprintPickleDebugLogger:
    @staticmethod
    def is_log_requested(config: ConfigFile) -> bool:
        return config.planner_log_path is not None

    @staticmethod
    def log_object_if_requested(
        config: ConfigFile, file_name_prefix: str, py_object: Any
    ) -> None:
        log_path = config.planner_log_path
        if log_path is None:
            return

        out_file_name = f"{file_name_prefix}_{_get_timestamp_str()}.pkl"
        with open(log_path / out_file_name, "wb") as file:
            pickle.dump(py_object, file)


def _get_timestamp_str() -> str:
    timestamp = datetime.datetime.now()
    timestamp = timestamp.astimezone(pytz.utc)  # UTC for consistency.
    return timestamp.strftime("%Y-%m-%d_%H-%M-%S")
