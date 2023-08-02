import boto3
import pytz
from typing import Iterator
from collections import namedtuple
from typing import Optional
from datetime import datetime, timedelta

from .common import TIMESTAMP_PREFIX_FORMAT
from brad.config.file import ConfigFile
from brad.utils.time_periods import timestamp_iterator, time_point_intersect


class LogFetcher:
    def __init__(self, config: ConfigFile) -> None:
        self._config = config
        self._s3 = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key,
            aws_secret_access_key=config.aws_access_key_secret,
        )

    def fetch_logs(
        self,
        window_start: datetime,
        window_end: datetime,
        include_contents: bool = True,
    ) -> Iterator["LogFile"]:
        """
        Returns all workload logs that fall within the given time window. Note
        that `window_end` is exclusive.
        """

        for batch_start_timestamp in timestamp_iterator(
            window_start, window_end, timedelta(hours=1)
        ):
            prefix = self._timestamp_to_prefix(batch_start_timestamp)
            for log_file_key in self._s3_list_objects_full(prefix):
                log_epoch_start = self._extract_epoch_start(log_file_key)

                # Check each log's start timestamp, since the results from S3
                # are not guaranteed to be sorted (since we have pagination
                # anyways).
                if not time_point_intersect(window_start, window_end, log_epoch_start):
                    continue

                if include_contents:
                    # Fetch the file from S3.
                    response = self._s3.get_object(
                        Bucket=self._config.s3_logs_bucket, Key=log_file_key
                    )
                    contents = response["Body"].read().decode("utf-8")
                else:
                    contents = None

                yield LogFile(log_file_key, log_epoch_start, contents)

    def _s3_list_objects_full(self, prefix: str) -> Iterator[str]:
        continuation_token: Optional[str] = None
        while True:
            obj_list = self._s3_list_objects_raw(prefix, continuation_token)
            for key in obj_list.keys:
                yield key
            if obj_list.is_truncated:
                assert obj_list.continuation_token is not None
                continuation_token = obj_list.continuation_token
            else:
                break

    def _s3_list_objects_raw(
        self, prefix: str, continuation_token: Optional[str]
    ) -> "_ObjectList":
        response = self._s3.list_objects_v2(
            Bucket=self._config.s3_logs_bucket,
            Prefix=self._config.s3_logs_path + prefix,
            ContinuationToken=continuation_token,
        )
        return _ObjectList(
            list(map(lambda f: f["Key"], response["Contents"])),
            response["IsTruncated"],
            response["NextContinuationToken"],
        )

    def _timestamp_to_prefix(self, timestamp: datetime) -> str:
        # We retrieve logs by the hour.
        return timestamp.strftime("%Y-%m-%d_%H")

    def _extract_epoch_start(self, file_s3_key: str) -> datetime:
        file_stem = file_s3_key.split("/")[-1]
        return datetime.strptime(
            " ".join(file_stem.split("_")[:2]), TIMESTAMP_PREFIX_FORMAT
        ).replace(tzinfo=pytz.utc)


LogFile = namedtuple("LogFile", ["file_key", "epoch_start", "contents"])

_ObjectList = namedtuple("_ObjectList", ["keys", "is_truncated", "continuation_token"])
