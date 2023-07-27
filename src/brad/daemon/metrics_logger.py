import pathlib
import pandas as pd

from datetime import datetime
from typing import Optional

from brad.config.file import ConfigFile


class MetricsLogger:
    """
    Used to incrementally log a metrics dataframe. This logger keeps track of
    the last logged timestamp and only logs new rows.
    """

    @classmethod
    def create_from_config(
        cls, config: ConfigFile, file_name: str
    ) -> Optional["MetricsLogger"]:
        log_path = config.metrics_log_path()
        if log_path is None:
            return None
        return cls(log_path / file_name)

    def __init__(self, file_path: pathlib.Path) -> None:
        self._last_logged_timestamp: Optional[datetime] = None
        self._fp = open(file_path, "w", encoding="UTF-8")

    def log_new_metrics(self, metrics: pd.DataFrame) -> None:
        if metrics.empty:
            return

        if self._last_logged_timestamp is None:
            metrics.to_csv(self._fp, mode="a", index_label="timestamp")
            self._last_logged_timestamp = metrics.index[-1]
        else:
            new_values = metrics.loc[metrics.index > self._last_logged_timestamp]
            if new_values.empty:
                return
            new_values.to_csv(self._fp, mode="a", header=False)
            self._last_logged_timestamp = new_values.index[-1]
