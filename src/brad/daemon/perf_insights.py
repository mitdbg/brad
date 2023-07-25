import boto3
import pytz
import pandas as pd
from typing import List, Optional
from datetime import datetime, timedelta, timezone

from brad.config.file import ConfigFile


class AwsPerformanceInsightsClient:
    def __init__(
        self, instance_identifier: str, config: Optional[ConfigFile] = None
    ) -> None:
        if config is not None:
            self._rds = boto3.client(
                "rds",
                aws_access_key_id=config.aws_access_key,
                aws_secret_access_key=config.aws_access_key_secret,
            )
            self._pi = boto3.client(
                "pi",
                aws_access_key_id=config.aws_access_key,
                aws_secret_access_key=config.aws_access_key_secret,
            )
        else:
            self._rds = boto3.client("rds")
            self._pi = boto3.client("pi")

        self._instance_id = self._fetch_instance_id(instance_identifier)

    def _fetch_instance_id(self, instance_identifier: str) -> str:
        response = self._rds.describe_db_instances(
            DBInstanceIdentifier=instance_identifier,
        )
        return response["DBInstances"][0]["DbiResourceId"]

    def fetch_metrics(
        self, metrics_list: List[str], period: timedelta, num_prev_points: int
    ) -> pd.DataFrame:
        # Retrieve datapoints
        now = datetime.now(tz=timezone.utc)
        end_time = now - (now - datetime.min.replace(tzinfo=pytz.UTC)) % period

        # Retrieve more than 1 epoch, for robustness; If we retrieve once per
        # minute and things are logged every minute, small delays might cause
        # us to miss some points. Deduplication is performed later on.
        start_time = end_time - num_prev_points * period

        def fetch_batch(metrics_list):
            metrics_queries = [{"Metric": metric} for metric in metrics_list]
            response = self._pi.get_resource_metrics(
                ServiceType="RDS",
                Identifier=self._instance_id,
                MetricQueries=metrics_queries,
                StartTime=start_time,
                EndTime=end_time,
                PeriodInSeconds=int(period.total_seconds()),
                PeriodAlignment="END_TIME",
            )

            # Initialize empty dictionary
            data_dict = {}

            # Iterate over JSON objects
            for obj in response["MetricList"]:
                metric = obj["Key"]["Metric"]
                data_points = obj["DataPoints"]
                for data_point in data_points:
                    timestamp = data_point["Timestamp"]
                    value = data_point.get("Value", float("nan"))
                    if timestamp not in data_dict:
                        data_dict[timestamp] = {}
                    data_dict[timestamp][metric] = value

            # Create dataframe from the dictionary
            df = pd.DataFrame.from_dict(data_dict, orient="index")

            # Sort dataframe by timestamp
            return df.sort_index()

        results = []
        batch_size = 10
        for i in range(0, len(metrics_list), batch_size):
            df = fetch_batch(metrics_list[i : i + batch_size])
            results.append(df)

        return pd.concat(results, axis=1)
