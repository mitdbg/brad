import asyncio
import boto3
import io
import logging
import os
import pathlib
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Deque

from .common import TIMESTAMP_PREFIX_FORMAT
from brad.utils.time_periods import period_start, universal_now

logger = logging.getLogger(__name__)


class EpochFileHandler(logging.Handler):
    def __init__(
        self,
        front_end_index: int,
        log_directory: pathlib.Path,
        epoch_length: timedelta,
        s3_logs_bucket: str,
        s3_logs_path: str,
        txn_log_prob: float,
    ) -> None:
        super().__init__()
        self._front_end_index = front_end_index
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

        self._files_to_upload: Deque[pathlib.Path] = deque()

    def emit(self, record: logging.LogRecord) -> None:
        query_timestamp = datetime.strptime(
            " ".join(record.getMessage().split(" ")[:2]), "%Y-%m-%d %H:%M:%S,%f"
        ).replace(tzinfo=timezone.utc)
        epoch_start = self._log_record_epoch_start(query_timestamp)

        if (
            self._current_epoch_start is not None
            and epoch_start < self._current_epoch_start
        ):
            # Drop the log record. We assume epochs always advance.
            logger.warning(
                "Dropping a log record for closed epoch starting at %s. Current epoch start: %s.",
                epoch_start,
                self._current_epoch_start,
            )
            return

        self._close_epoch_and_advance_if_needed(epoch_start)

        log_entry = self.format(record)
        if self._log_file_t and self._log_file_a:
            if record.getMessage().strip().split(" ")[-1] == "True":
                self._log_file_t.write(log_entry + "\n")
                self._log_file_t.flush()
            else:
                self._log_file_a.write(log_entry + "\n")
                self._log_file_a.flush()

    async def refresh(self) -> None:
        """
        Meant to be called periodically (once an "epoch") to do any cleanup
        tasks (e.g., closing the current epoch, uploading files).
        """
        epoch_start = period_start(universal_now(), self._epoch_length)
        self._close_epoch_and_advance_if_needed(epoch_start)
        await self._do_uploads()

    def _get_log_file_paths(self) -> Tuple[pathlib.Path, pathlib.Path]:
        assert self._current_epoch_start is not None
        formatted_epoch_start = self._current_epoch_start.strftime(
            TIMESTAMP_PREFIX_FORMAT
        )
        return (
            self._log_directory
            / f"{formatted_epoch_start}_{self._front_end_index}_transactional_p{int(self._txn_log_prob*100)}.log"
        ), (
            self._log_directory
            / f"{formatted_epoch_start}_{self._front_end_index}_analytical.log"
        )

    def _upload_to_s3(self, local_file_path: str):
        self._s3_client.upload_file(
            local_file_path,
            self._s3_logs_bucket,
            os.path.join(self._s3_logs_path, os.path.basename(local_file_path)),
        )

    def _log_record_epoch_start(self, timestamp: datetime) -> datetime:
        return period_start(timestamp, self._epoch_length)

    def _close_epoch_and_advance_if_needed(self, next_epoch_start: datetime) -> None:
        if (self._current_epoch_start is not None) and (
            not (next_epoch_start > self._current_epoch_start)
        ):
            # No need to close the current epoch.
            return

        # Close the previous log file handler, if any, and schedule for upload to S3.
        if self._log_file_t and self._log_file_a:
            self._log_file_t.close()
            self._log_file_a.close()
            log_file_path_t, log_file_path_a = self._get_log_file_paths()

            # Defer the upload to later (it should not run on the critical path).
            self._files_to_upload.append(log_file_path_t)
            self._files_to_upload.append(log_file_path_a)

        # Format the epoch start as a string for the new log filename
        self._current_epoch_start = next_epoch_start
        log_file_path_t, log_file_path_a = self._get_log_file_paths()

        # Create new log file handlers for the new epoch
        self._log_file_t = open(log_file_path_t, "a+", encoding="UTF-8")
        self._log_file_a = open(log_file_path_a, "a+", encoding="UTF-8")

    async def _do_uploads(self) -> None:
        loop = asyncio.get_running_loop()

        while len(self._files_to_upload) > 0:
            to_upload = self._files_to_upload.popleft()
            logger.debug("Uploading log to S3: %s", to_upload)
            # Ideally we should run these uploads in parallel. However, the
            # boto3 client probably cannot be used across threads, which would
            # increase the complexity of this code.
            await loop.run_in_executor(None, self._upload_to_s3, to_upload)

            # Safe to delete now.
            to_upload.unlink()
