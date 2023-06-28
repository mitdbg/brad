import json
import boto3
import pandas as pd
import numpy as np
import pytz
import time

from importlib.resources import files, as_file
from datetime import datetime, timedelta, timezone
from typing import List

from brad.config.engine import Engine
import brad.daemon as daemon


# Return the id of a metric in the dataframe.
def get_metric_id(engine: str, metric_name: str, stat: str, role: str = ""):
    metric_id = f"{engine}_{metric_name}_{stat}"
    if role != "":
        metric_id = f"{engine}_{role}_{metric_name}_{stat}"
    return metric_id


class MetricsHelper:
    SERVICE_DICT = {
        "Amazon Redshift": "redshift",
        "Amazon Relational Database Service": "aurora",
        "Amazon Simple Storage Service": "s3",
    }

    def __init__(
        self,
        aurora_cluster_name: str,
        redshift_cluster_name: str,
        epoch_length: timedelta,
    ) -> None:
        self._cluster_ids = {}
        self._cluster_ids[Engine.Aurora] = aurora_cluster_name
        self._cluster_ids[Engine.Redshift] = redshift_cluster_name
        self._epoch_length = epoch_length
        self._setup()
        self._values = pd.DataFrame(columns=self._metric_ids)
        self._client = boto3.client("cloudwatch")

    def load_metrics(self) -> None:
        self._add_metrics()

    ############
    # The following functions, prefixed by `read_`, provide different ways to query the monitor for
    # the values of the metrics of interest. They return a dataframe with a schema that looks like the following:
    #
    #                            redshift_CPUUtilization_Average  aurora_WRITER_ReadLatency_Maximum  athena_ProcessedBytes_Sum
    # 2023-04-25 00:00:00+00:00                         3.191965                                0.0                        0.0
    # 2023-04-26 00:00:00+00:00                         3.198332                                0.0                        0.0
    # 2023-04-27 00:00:00+00:00                         3.173024                                0.0                        0.0
    #
    # The indices of these dataframes consist of timestamps associated with each epoch.
    # Each column name is a `metric_id`, consisting of up to 4 underscore-separated fields:
    #   1. The engine name, from the `engine` field in `monitored_metrics`, within `monitored_metrics.json`
    #   2. For aurora, the instance role, from the `roles` field in `monitored_metrics`, within `monitored_metrics.json`
    #   3. The metric name, a key in the `metrics` field in `monitored_metrics` within `monitored_metrics.json`
    #   4. The reported statistic name, an element within a value in the `metrics` field in `monitored_metrics` within
    #      `monitored_metrics.json`
    #

    def get_metrics(self, metric_ids: List[str] | None = None) -> pd.DataFrame:
        if self._values.empty:
            return self._values

        columns = metric_ids if metric_ids else list(self._values.columns)

        return self._values[columns]

    # `end_ts` is inclusive
    def read_upcoming_until(
        self, end_ts: datetime, metric_ids: List[str] | None = None
    ) -> pd.DataFrame:
        if self._values.empty:
            return self._values

        k = (end_ts - self._values.index[-1]) // self._epoch_length
        return self.read_k_upcoming(k, metric_ids)

    # Both ends inclusive
    def read_between_times(
        self,
        start_time: datetime,
        end_time: datetime,
        metric_ids: List[str] | None = None,
    ) -> pd.DataFrame:
        if self._values.empty:
            return self._values

        past = self._values.loc[
            (self._values.index >= start_time) & (self._values.index <= end_time)
        ]
        future = self.read_upcoming_until(end_time, metric_ids)

        return pd.concat([past, future], axis=0)

    # Both ends inclusive
    def read_between_epochs(self, start_epoch: int, end_epoch: int) -> pd.DataFrame:
        if self._values.empty:
            return self._values

        past = self.read_k_most_recent(max(0, -start_epoch)).head(
            end_epoch - start_epoch + 1
        )
        future = self.read_k_upcoming(max(0, end_epoch + 1)).tail(
            end_epoch - start_epoch + 1
        )

        return pd.concat([past, future], axis=0)

    ############

    def _setup(self):
        # Load data for monitored metrics.
        metrics_file = files(daemon).joinpath("monitored_metrics.json")
        with as_file(metrics_file) as file:
            with open(file, "r", encoding="utf8") as data:
                file_contents = json.load(data)

        # Create the cloudwatch queries and list the metric ids used
        self._queries = []
        self._metric_ids = []
        for f in file_contents["monitored_metrics"]:
            try:
                engine = Engine.from_str(f["engine"])
            except ValueError:
                continue

            namespace = ""
            dimensions = []
            if engine == Engine.Aurora:
                namespace = "AWS/RDS"
                dimensions = [
                    {
                        "Name": "DBClusterIdentifier",
                        "Value": self._cluster_ids[Engine.Aurora],
                    },
                    {},  # Gets set in the loop.
                ]
            elif engine == Engine.Redshift:
                namespace = "AWS/Redshift"
                dimensions = [
                    {
                        "Name": "ClusterIdentifier",
                        "Value": self._cluster_ids[Engine.Redshift],
                    },
                ]
            elif engine == Engine.Athena:
                namespace = "AWS/Athena"
                dimensions = [
                    # TODO: Restrict metrics to an Athena workgroup.
                    # We do not do so right now because the bootstrap workflow does not
                    # set up an Athena workgroup.
                    # {
                    #     "Name": "WorkGroup",
                    #     "Value": self._cluster_ids[Engine.Athena],
                    # }
                ]

            roles = f.get("roles", [""])
            for role in roles:
                for metric_name, stats_list in f["metrics"].items():
                    for stat in stats_list:
                        metric_id = f"{engine.value}_{metric_name}_{stat}"
                        if role != "":
                            metric_id = f"{engine.value}_{role}_{metric_name}_{stat}"
                            dimensions[1] = {"Name": "Role", "Value": role}
                        self._metric_ids.append(metric_id)
                        metric_data_query = {
                            "Id": metric_id,
                            "MetricStat": {
                                "Metric": {
                                    "Namespace": namespace,
                                    "MetricName": metric_name,
                                    "Dimensions": dimensions.copy(),
                                },
                                "Period": int(self._epoch_length.total_seconds()),
                                "Stat": stat,
                            },
                            "ReturnData": True,
                        }
                        self._queries.append(metric_data_query)

    def _add_metrics(self):
        # Retrieve datapoints
        now = datetime.now(tz=timezone.utc)
        end_time = (
            now - (now - datetime.min.replace(tzinfo=pytz.UTC)) % self._epoch_length
        )

        # Retrieve more than 1 epoch, for robustness; If we retrieve once per
        # minute and things are logged every minute, small delays might cause
        # us to miss some points. Deduplication is performed later on.
        start_time = end_time - 20 * self._epoch_length

        if not self._values.empty:
            start_time = self._values.index[-1]

        response_cloudwatch = self._client.get_metric_data(
            MetricDataQueries=self._queries,
            StartTime=start_time,
            EndTime=end_time,
            ScanBy="TimestampAscending",
        )

        # Parse metrics from json responses
        resp_dict = {}
        for metric_data in response_cloudwatch["MetricDataResults"]:
            metric_id = metric_data["Id"]
            metric_timestamps = metric_data["Timestamps"]
            metric_values = metric_data["Values"]
            resp_dict[metric_id] = pd.Series(
                metric_values, index=metric_timestamps, dtype=np.float64
            )

        df = pd.DataFrame(resp_dict).fillna(0)
        df = df.sort_index()
        df.index = pd.to_datetime(df.index)

        for metric_id in self._metric_ids:
            if metric_id not in df.columns:
                df[metric_id] = pd.Series(dtype=np.float64)

        # Append only the new rows to the internal representation
        self._values = (
            df.copy()
            if self._values.empty
            else pd.concat([self._values, df.loc[df.index > self._values.index[-1]]])
        )


if __name__ == "__main__":
    metrics_reader = MetricsHelper(
        aurora_cluster_name="aurora-2",
        redshift_cluster_name="redshift-ra3-test",
        epoch_length=timedelta(minutes=3),
    )

    while True:
        metrics_reader.load_metrics()
        df = metrics_reader.get_metrics(["aurora_WRITER_DatabaseConnections_Average"])
        print(df)
        time.sleep(10)
