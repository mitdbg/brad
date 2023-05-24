import logging
from datetime import datetime, timedelta, timezone
import os
import io
import boto3
from typing import Optional, Tuple


class EpochFileHandler(logging.Handler):
    def __init__(
        self,
        log_directory: str,
        epoch_length: timedelta,
        s3_logs_bucket: str,
        s3_logs_path: str,
        txn_log_prob: float,
    ) -> None:
        super().__init__()
        self._current_epoch_start: Optional[datetime] = None
        self._log_directory = log_directory
        self._log_file_t: Optional[io.TextIOWrapper] = None
        self._log_file_a: Optional[io.TextIOWrapper] = None
        os.makedirs(self._log_directory, exist_ok=True)
        self._epoch_length = epoch_length
        self._s3_client = boto3.client("s3")
        self._s3_logs_bucket = s3_logs_bucket
        self._s3_logs_path = s3_logs_path
        self._txn_log_prob = txn_log_prob

    def emit(self, record: logging.LogRecord) -> None:
        query_timestamp = datetime.strptime(
            " ".join(record.getMessage().split(" ")[:2]), "%Y-%m-%d %H:%M:%S,%f"
        ).replace(tzinfo=timezone.utc)

        epoch_start = (
            query_timestamp
            - (query_timestamp - datetime.min.replace(tzinfo=timezone.utc))
            % self._epoch_length
        )

        if epoch_start != self._current_epoch_start:
            # Close the previous log file handler, if any, and upload to S3.
            if self._log_file_t and self._log_file_a:
                self._log_file_t.close()
                self._log_file_a.close()
                log_file_path_t, log_file_path_a = self.get_log_file_paths()
                self.upload_to_s3(log_file_path_t)
                self.upload_to_s3(log_file_path_a)

            # Format the epoch start as a string for the new log filename
            self._current_epoch_start = epoch_start
            log_file_path_t, log_file_path_a = self.get_log_file_paths()

            # Create new log file handlers for the new epoch
            self._log_file_t = open(log_file_path_t, "a+", encoding="UTF-8")
            self._log_file_a = open(log_file_path_a, "a+", encoding="UTF-8")

        log_entry = self.format(record)
        if self._log_file_t and self._log_file_a:
            if record.getMessage().strip().split(" ")[-1] == "True":
                self._log_file_t.write(log_entry + "\n")
                self._log_file_t.flush()
            else:
                self._log_file_a.write(log_entry + "\n")
                self._log_file_a.flush()

    def get_log_file_paths(self) -> Tuple[str, str]:
        if self._current_epoch_start:
            formatted_epoch_start = self._current_epoch_start.strftime(
                "%Y-%m-%d_%H:%M:%S"
            )
            return os.path.join(
                self._log_directory,
                f"{formatted_epoch_start}_transactional_p{int(self._txn_log_prob*100)}.log",
            ), os.path.join(
                self._log_directory, f"{formatted_epoch_start}_analytical.log"
            )
        else:
            return "", ""

    def upload_to_s3(self, local_file_path: str):
        self._s3_client.upload_file(
            local_file_path,
            self._s3_logs_bucket,
            os.path.join(self._s3_logs_path, os.path.basename(local_file_path)),
        )
