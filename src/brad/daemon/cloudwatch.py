import boto3
import pytz
import numpy as np
import pandas as pd
from typing import List, Optional, Tuple
from datetime import datetime, timedelta, timezone

from brad.config.engine import Engine
from brad.config.file import ConfigFile


class CloudwatchClient:
    def __init__(
        self,
        engine: Engine,
        cluster_identifier: str,
        config: Optional[ConfigFile] = None,
    ) -> None:
        self._engine = engine
        self._dimensions = []

        if self._engine == Engine.Aurora:
            self._namespace = "AWS/RDS"
            self._dimensions.append(
                {
                    "Name": "DBClusterIdentifier",
                    "Value": cluster_identifier,
                }
            )
        elif self._engine == Engine.Athena:
            self._namespace = "AWS/Athena"
        elif self._engine == Engine.Redshift:
            self._namespace = "AWS/Redshift"
            self._dimensions.append(
                {
                    "Name": "ClusterIdentifier",
                    "Value": cluster_identifier,
                }
            )

        if config is not None:
            self._client = boto3.client(
                "cloudwatch",
                aws_access_key_id=config.aws_access_key,
                aws_secret_access_key=config.aws_access_key_secret,
            )
        else:
            self._client = boto3.client("cloudwatch")

    def fetch_metrics(
        self,
        metrics_list: List[Tuple[str, str]],
        period: timedelta,
        num_prev_points: int,
    ) -> pd.DataFrame:
        # Retrieve datapoints
        now = datetime.now(tz=timezone.utc)
        end_time = now - (now - datetime.min.replace(tzinfo=pytz.UTC)) % period

        # Retrieve more than 1 epoch, for robustness; If we retrieve once per
        # minute and things are logged every minute, small delays might cause
        # us to miss some points. Deduplication is performed later on.
        start_time = end_time - num_prev_points * period

        def fetch_batch(metrics_list):
            queries = []
            for metric, stat in metrics_list:
                queries.append(
                    {
                        "Id": f"{metric}_{stat}",
                        "MetricStat": {
                            "Metric": {
                                "Namespace": self._namespace,
                                "MetricName": metric,
                                "Dimensions": self._dimensions.copy(),
                            },
                            "Period": int(period.total_seconds()),
                            "Stat": stat,
                        },
                        "ReturnData": True,
                    }
                )

            response = self._client.get_metric_data(
                MetricDataQueries=queries,
                StartTime=start_time,
                EndTime=end_time,
                ScanBy="TimestampAscending",
            )

            # Parse metrics from json responses
            resp_dict = {}
            for metric_data in response["MetricDataResults"]:
                metric_id = metric_data["Id"]
                metric_timestamps = metric_data["Timestamps"]
                metric_values = metric_data["Values"]
                resp_dict[metric_id] = pd.Series(
                    metric_values, index=metric_timestamps, dtype=np.float64
                )

            df = pd.DataFrame(resp_dict).fillna(0)
            df = df.sort_index()
            df.index = pd.to_datetime(df.index)

            # Sort dataframe by timestamp
            return df.sort_index()

        results = []
        batch_size = 10
        for i in range(0, len(metrics_list), batch_size):
            df = fetch_batch(metrics_list[i : i + batch_size])
            results.append(df)

        return pd.concat(results, axis=1)
