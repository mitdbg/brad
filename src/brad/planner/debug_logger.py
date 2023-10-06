import csv
import datetime
import pathlib
import pytz
from typing import Optional, Dict, List

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
        timestamp = datetime.datetime.now()
        timestamp = timestamp.astimezone(pytz.utc)  # UTC for consistency.
        curr_time = timestamp.strftime("%Y-%m-%d_%H-%M-%S")
        out_file_name = f"{file_name_prefix}_{curr_time}.csv"
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
