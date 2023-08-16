import csv
import pathlib
import pytz
from datetime import datetime
from typing import Optional

from brad.config.file import ConfigFile
from brad.config.system_event import SystemEvent


class SystemEventLogger:
    """
    Used to log system events (that occur on the daemon) for later analysis.
    """

    @classmethod
    def create_if_requested(cls, config: ConfigFile) -> Optional["SystemEventLogger"]:
        daemon_log_path = config.daemon_log_path
        if daemon_log_path is not None:
            return cls(daemon_log_path)
        else:
            return None

    def __init__(self, log_path: pathlib.Path) -> None:
        self._log_path = log_path / "brad_daemon_events.csv"
        self._headers = ["timestamp", "event", "extra_details"]
        self._file = open(self._log_path, "a", encoding="UTF-8")
        self._csv_writer = csv.writer(self._file)
        self._logged_header = False

    def log(self, event: SystemEvent, extra_details: Optional[str] = None) -> None:
        if not self._logged_header:
            self._csv_writer.writerow(self._headers)
            self._logged_header = True

        now = datetime.now().replace(tzinfo=pytz.utc)
        self._csv_writer.writerow(
            [
                now.strftime("%Y-%m-%d %H:%M:%S"),
                event.value,
                extra_details if extra_details is not None else "",
            ]
        )
