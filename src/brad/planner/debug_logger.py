import csv
import os
import datetime
from typing import Optional, Dict, List

LOG_REPLAN_VAR = "BRAD_LOG_PLANNING"


class BlueprintPlanningDebugLogger:
    """
    Used for debugging purposes.
    """

    @classmethod
    def create_if_requested(
        cls, prefix: str
    ) -> Optional["BlueprintPlanningDebugLogger"]:
        if LOG_REPLAN_VAR not in os.environ:
            return None
        return cls(prefix)

    def __init__(self, prefix: str) -> None:
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        out_file_name = f"{prefix}_{curr_time}.csv"
        self._out_file = open(out_file_name, "w", encoding="UTF-8")
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

        row = []
        for key in self._key_order:
            row.append(values[key])
        writer.writerow(row)
        self._out_file.flush()
