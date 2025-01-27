import boto3
import pytz
import logging
import pandas as pd
from botocore.exceptions import ClientError
from typing import List, Optional
from datetime import datetime, timedelta

from .metrics_def import MetricDef
from brad.config.file import ConfigFile
from brad.utils.time_periods import universal_now

logger = logging.getLogger(__name__)


class PerfInsightsClient:
    @classmethod
    def from_instance_identifier(
        cls, instance_identifier: str, config: Optional[ConfigFile] = None
    ) -> "PerfInsightsClient":
        if config is not None:
            rds = boto3.client(
                "rds",
                aws_access_key_id=config.aws_access_key,
                aws_secret_access_key=config.aws_access_key_secret,
            )
        else:
            rds = boto3.client("rds")

        response = rds.describe_db_instances(
            DBInstanceIdentifier=instance_identifier,
        )
        resource_id = response["DBInstances"][0]["DbiResourceId"]

        return cls(resource_id, config)

    def __init__(self, resource_id: str, config: Optional[ConfigFile] = None) -> None:
        if config is not None:
            self._pi = boto3.client(
                "pi",
                aws_access_key_id=config.aws_access_key,
                aws_secret_access_key=config.aws_access_key_secret,
            )
        else:
            self._pi = boto3.client("pi")
        self._resource_id = resource_id

    @staticmethod
    def metric_names(metric_defs: List[MetricDef]) -> List[str]:
        return list(map(lambda m: "{}.{}".format(*m), metric_defs))

    def fetch_metrics(
        self, metrics_list: List[MetricDef], period: timedelta, num_prev_points: int
    ) -> pd.DataFrame:
        # Retrieve datapoints
        now = universal_now()
        end_time = now - (now - datetime.min.replace(tzinfo=pytz.UTC)) % period

        # Retrieve more than 1 epoch, for robustness; If we retrieve once per
        # minute and things are logged every minute, small delays might cause
        # us to miss some points. Deduplication is performed later on.
        start_time = end_time - num_prev_points * period

        def fetch_batch(metrics_list):
            metrics_queries = [
                {"Metric": "{}.{}".format(*metric)} for metric in metrics_list
            ]
            response = self._pi.get_resource_metrics(
                ServiceType="RDS",
                Identifier=self._resource_id,
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
                    # Ensure we operate in UTC for consistency across all our
                    # metrics handling.
                    timestamp = timestamp.astimezone(pytz.utc)
                    value = data_point.get("Value", float("nan"))
                    if timestamp not in data_dict:
                        data_dict[timestamp] = {}
                    data_dict[timestamp][metric] = value

            # Create dataframe from the dictionary
            df = pd.DataFrame.from_dict(data_dict, orient="index")

            # Sort dataframe by timestamp
            return df.sort_index()

        try:
            results = []
            batch_size = 10
            for i in range(0, len(metrics_list), batch_size):
                df = fetch_batch(metrics_list[i : i + batch_size])
                results.append(df)

            return pd.concat(results, axis=1)
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "EntityAlreadyExists":
                logger.info(
                    "Received PerfInsights unauthorized error for %s. This might be due to a transition.",
                    self._resource_id,
                )
            else:
                logger.exception("Unexpected PerfInsights error.")

            # Return an empty DataFrame.
            return pd.DataFrame(
                columns=list(map(lambda m: "{}.{}".format(*m), metrics_list))
            )
