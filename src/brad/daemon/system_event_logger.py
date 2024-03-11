import csv
import pathlib
from datetime import datetime
from typing import Optional, Deque, Tuple, List
from collections import deque

from brad.config.file import ConfigFile
from brad.config.system_event import SystemEvent
from brad.utils.time_periods import universal_now

SystemEventRecord = Tuple[datetime, SystemEvent, str]


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
        self._memlog: Deque[Tuple[datetime, SystemEvent, str]] = deque()
        self._memlog_maxlen = 100

    def log(self, event: SystemEvent, extra_details: Optional[str] = None) -> None:
        if not self._logged_header:
            self._csv_writer.writerow(self._headers)
            self._logged_header = True

        now = universal_now()
        row = (
            now,
            event,
            extra_details if extra_details is not None else "",
        )

        if len(self._memlog) == self._memlog_maxlen:
            self._memlog.popleft()
        self._memlog.append(row)

        self._csv_writer.writerow(
            [
                row[0].strftime("%Y-%m-%d %H:%M:%S"),
                row[1].value,
                row[2],
            ]
        )
        self._file.flush()

    def current_memlog(self) -> List[SystemEventRecord]:
        """
        Used for retrieving the system event log.
        """
        return list(self._memlog)
