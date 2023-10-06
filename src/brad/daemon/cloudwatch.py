import boto3
import pytz
import numpy as np
import pandas as pd
import logging
from typing import List, Optional, Tuple
from datetime import datetime, timedelta, timezone

from .metrics_def import MetricDef
from brad.config.engine import Engine
from brad.config.file import ConfigFile

logger = logging.getLogger(__name__)


class CloudWatchClient:
    def __init__(
        self,
        engine: Engine,
        cluster_identifier: Optional[str],
        instance_identifier: Optional[str],
        config: Optional[ConfigFile] = None,
    ) -> None:
        self._engine = engine
        self._dimensions = []

        assert (
            cluster_identifier is not None or instance_identifier is not None
        ), "Must provide a cluster or instance identifier."

        if self._engine == Engine.Aurora:
            self._namespace = "AWS/RDS"
            if cluster_identifier is not None:
                self._dimensions.append(
                    {
                        "Name": "DBClusterIdentifier",
                        "Value": cluster_identifier,
                    }
                )
            else:
                assert instance_identifier is not None
                self._dimensions.append(
                    {
                        "Name": "DBInstanceIdentifier",
                        "Value": instance_identifier,
                    }
                )
        elif self._engine == Engine.Athena:
            self._namespace = "AWS/Athena"
        elif self._engine == Engine.Redshift:
            assert (
                cluster_identifier is not None
            ), "Must provide cluster ID for Redshift."
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

    @staticmethod
    def metric_names(metric_defs: List[MetricDef]) -> List[str]:
        return list(map(lambda m: "{}_{}".format(*m), metric_defs))

    def fetch_metrics(
        self,
        metrics_list: List[Tuple[str, str]],
        period: timedelta,
        num_prev_points: int,
    ) -> pd.DataFrame:
        """
        Retrieves metrics from CloudWatch. Note that some metric values may be
        NaN: this indicates that the value is not available, but may become
        available later.
        """

        now = datetime.now(tz=timezone.utc)
        end_time = now - (now - datetime.min.replace(tzinfo=pytz.UTC)) % period

        # Retrieve more than 1 epoch, for robustness; If we retrieve once per
        # minute and things are logged every minute, small delays might cause
        # us to miss some points. Deduplication is performed later on.
        start_time = end_time - num_prev_points * period
        logger.debug(
            "Querying CloudWatch using the range %s -- %s", start_time, end_time
        )

        def fetch_batch(metrics_list):
            queries = []
            for metric, stat in metrics_list:
                queries.append(
                    {
                        # CloudWatch expects this ID to start with a lowercase
                        # character.
                        "Id": f"a{metric}_{stat}",
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
                metric_id = metric_data["Id"][1:]
                metric_timestamps = metric_data["Timestamps"]
                metric_values = metric_data["Values"]
                resp_dict[metric_id] = pd.Series(
                    metric_values, index=metric_timestamps, dtype=np.float64
                )

            df = pd.DataFrame(resp_dict)
            df = df.sort_index()
            df.index = pd.to_datetime(df.index, utc=True, unit="ns")

            for metric_def in metrics_list:
                metric_name = "{}_{}".format(*metric_def)
                if metric_name in df.columns:
                    continue
                # Missing metric value.
                df.loc[metric_name] = pd.Series(dtype=np.float64)

            # Sort dataframe by timestamp
            return df.sort_index()

        results = []
        batch_size = 10
        for i in range(0, len(metrics_list), batch_size):
            df = fetch_batch(metrics_list[i : i + batch_size])
            results.append(df)

        return pd.concat(results, axis=1)
