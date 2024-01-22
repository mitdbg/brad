import boto3
import pytz
import numpy as np
import pandas as pd
import logging
from typing import List, Optional, Dict, Tuple
from datetime import datetime, timedelta

from .metrics_def import MetricDef
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.utils.time_periods import universal_now

logger = logging.getLogger(__name__)

# We only collect metrics for up to 16 Redshift nodes.
MAX_REDSHIFT_NODES = 16


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
        self._is_for_redshift = False

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
            self._is_for_redshift = True

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
        metrics_list: List[MetricDef],
        period: timedelta,
        num_prev_points: int,
    ) -> pd.DataFrame:
        """
        Retrieves metrics from CloudWatch. Note that some metric values may be
        NaN: this indicates that the value is not available, but may become
        available later.
        """

        now = universal_now()
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
            for metric, stat, dimension_info in metrics_list:
                dimensions = self._dimensions.copy()

                # CloudWatch expects this ID to start with a lowercase
                # character.
                if dimension_info is None:
                    id_to_use = f"a{metric}_{stat}"
                else:
                    id_to_use = f"a{metric}_{stat}_{dimension_info['InternalValue']}"
                    dimensions.append(
                        {
                            "Name": dimension_info["CloudwatchName"],
                            "Value": dimension_info["CloudwatchValue"],
                        }
                    )

                queries.append(
                    {
                        "Id": id_to_use,
                        "MetricStat": {
                            "Metric": {
                                "Namespace": self._namespace,
                                "MetricName": metric,
                                "Dimensions": dimensions,
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
                metric_timestamps = pd.to_datetime(
                    metric_data["Timestamps"], utc=True, unit="ns"
                )
                metric_values = metric_data["Values"]
                resp_dict[metric_id] = pd.Series(
                    metric_values, index=metric_timestamps, dtype=np.float64
                )

            df = pd.DataFrame(resp_dict)
            df = df.sort_index()

            for metric_def in metrics_list:
                metric, stat, dimension_info = metric_def
                if dimension_info is None:
                    metric_name = "{}_{}".format(*metric_def)
                else:
                    metric_name = "{}_{}_{}".format(
                        metric, stat, dimension_info["InternalValue"]
                    )
                if metric_name in df.columns:
                    continue
                # Missing metric value.
                df.loc[metric_name] = pd.Series(dtype=np.float64)

            # Sort dataframe by timestamp
            return df.sort_index()

        results = []
        batch_size = 10
        metrics_list_internal: List[Tuple[str, str, Optional[Dict[str, str]]]] = []
        for metric, stat in metrics_list:
            metrics_list_internal.append((metric, stat, None))

            # We fetch additional per-node metrics when working with Redshift.
            if self._is_for_redshift and metric == "CPUUtilization":
                for dimension in _REDSHIFT_NODE_DIMENSIONS:
                    metrics_list_internal.append((metric, stat, dimension))

        for i in range(0, len(metrics_list_internal), batch_size):
            df = fetch_batch(metrics_list_internal[i : i + batch_size])
            results.append(df)

        return pd.concat(results, axis=1)


# Ideally these should be configurable by the client's user. To avoid changing
# our abstraction in the short term, we use this now and will refactor later.
_REDSHIFT_NODE_DIMENSIONS = [
    {
        "CloudwatchName": "NodeID",
        "CloudwatchValue": "Leader",
        "InternalValue": "Leader",
    },
    {
        "CloudwatchName": "NodeID",
        "CloudwatchValue": "Shared",
        "InternalValue": "Shared",
    },
] + [
    {
        "CloudwatchName": "NodeID",
        "CloudwatchValue": f"Compute-{node_num}",
        "InternalValue": f"Compute{node_num}",
    }
    for node_num in range(MAX_REDSHIFT_NODES)
]
