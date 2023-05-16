from brad.config.file import ConfigFile
from importlib.resources import files, as_file
from typing import List, Dict
import json
from brad.config.engine import Engine
import brad.daemon as daemon
from datetime import datetime, timedelta, timezone
import boto3
import pandas as pd
import asyncio
import numpy as np
import pytz
from brad.forecasting.constant_forecaster import ConstantForecaster
from brad.forecasting.moving_average_forecaster import MovingAverageForecaster
from brad.forecasting.linear_forecaster import LinearForecaster
from brad.forecasting import Forecaster


# Return the id of a metric in the dataframe.
def get_metric_id(engine: str, metric_name: str, stat: str, role: str = ""):
    metric_id = f"{engine}_{metric_name}_{stat}"
    if role != "":
        metric_id = f"{engine}_{role}_{metric_name}_{stat}"
    return metric_id


# Monitor
class Monitor:
    SERVICE_DICT = {
        "Amazon Redshift": "redshift",
        "Amazon Relational Database Service": "aurora",
        "Amazon Simple Storage Service": "s3",
    }

    def __init__(
        self,
        cluster_ids: Dict[Engine, str],
        forecasting_method: str = "constant",
        forecasting_window_size: int = 5,  # (Up to) how many past samples to base the forecast on
        forecasting_epoch: timedelta = timedelta(hours=1),
        enable_cost_monitoring: bool = False,
    ) -> None:
        self._cluster_ids = cluster_ids
        self._epoch_length = forecasting_epoch
        self._enable_cost_monitoring = enable_cost_monitoring
        self._setup()
        self._values = pd.DataFrame(columns=self._metric_ids)

        self._client = boto3.client("cloudwatch")
        if self._enable_cost_monitoring:
            self._cost_client = boto3.client("ce")

        self._forecaster: Forecaster
        if forecasting_method == "constant":
            self._forecaster = ConstantForecaster(self._values, self._epoch_length)
        elif forecasting_method == "moving_average":
            self._forecaster = MovingAverageForecaster(
                self._values, self._epoch_length, forecasting_window_size
            )
        elif forecasting_method == "linear":
            self._forecaster = LinearForecaster(
                self._values, self._epoch_length, forecasting_window_size
            )

    # Forcibly read metrics. Use to avoid `run_forever()`.
    def force_read_metrics(self) -> None:
        self._add_metrics()

    # Create from config file.
    @classmethod
    def from_config_file(cls, config: ConfigFile):
        cluster_ids = config.get_cluster_ids()
        return cls(cluster_ids)

    # Create from schema name.
    @classmethod
    def from_schema_name(cls, schema_name: str):
        cluster_ids = {
            Engine.Redshift: f"brad-{schema_name}",
            Engine.Aurora: f"brad-{schema_name}",
            Engine.Athena: f"brad-{schema_name}",
        }
        return cls(cluster_ids)

    async def run_forever(self) -> None:
        # Flesh out the monitor - maintain running averages of the underlying
        # engines' metrics.
        while True:
            self._add_metrics()
            await asyncio.sleep(self._epoch_length.total_seconds())  # Read every epoch

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

    def read_k_most_recent(
        self, k: int = 1, metric_ids: List[str] | None = None
    ) -> pd.DataFrame:
        if self._values.empty:
            return self._values

        columns = metric_ids if metric_ids else list(self._values.columns)

        return self._values.tail(k)[columns]

    def read_k_upcoming(
        self, k: int = 1, metric_ids: List[str] | None = None
    ) -> pd.DataFrame:
        if self._values.empty:
            return self._values

        # Create empty dataframe with desired index and columns
        timestamps = [
            self._values.index[-1] + i * self._epoch_length for i in range(1, k + 1)
        ]
        columns = metric_ids if metric_ids else self._values.columns
        df = pd.DataFrame(index=timestamps, columns=columns)

        # Fill in the values
        for col in columns:
            vals = self._forecaster.num_points(col, k)
            df[col] = vals

        return df

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
        metrics_file = files(daemon).joinpath("test_monitored_metrics.json")
        with as_file(metrics_file) as file:
            with open(file, "r", encoding="utf8") as data:
                file_contents = json.load(data)

        if self._enable_cost_monitoring and self._epoch_length < timedelta(days=1):
            raise ValueError(
                "When cost monitoring is enabled, the epoch length must be no less than the cost monitoring period: 1 day"
            )

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
                    {
                        "Name": "WorkGroup",
                        "Value": self._cluster_ids[Engine.Athena],
                    }
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

        # Load data for monitored costs.
        if self._enable_cost_monitoring:
            cost_file = files(daemon).joinpath("monitored_costs.json")
            with as_file(cost_file) as file:
                with open(file, "r", encoding="utf8") as data:
                    self._cost_query_fields = json.load(data)

            try:
                services_short = [
                    self.SERVICE_DICT[s] for s in self._cost_query_fields["services"]
                ]
            except KeyError as exc:
                raise ValueError(
                    "Invalid service specified in `monitored_costs.json"
                ) from exc

            for s in services_short:
                for m in self._cost_query_fields["metrics"]:
                    metric_id = f"{s}_{m}"
                    self._metric_ids.append(metric_id)

    def _add_metrics(self):
        # Retrieve datapoints
        now = datetime.now(tz=timezone.utc)
        end_time = (
            now - (now - datetime.min.replace(tzinfo=pytz.UTC)) % self._epoch_length
        )

        # Retrieve more than 1 epoch, for robustness; If we retrieve once per
        # minute and things are logged every minute, small delays might cause
        # us to miss some points. Deduplication is performed later on.
        start_time = end_time - 3 * self._epoch_length

        if not self._values.empty:
            start_time = self._values.index[-1]

        response_cloudwatch = self._client.get_metric_data(
            MetricDataQueries=self._queries,
            StartTime=start_time,
            EndTime=end_time,
            ScanBy="TimestampAscending",
        )
        if self._enable_cost_monitoring:
            response_cost = self._cost_client.get_cost_and_usage(
                TimePeriod={
                    "Start": datetime.strftime(start_time, "%Y-%m-%d"),
                    "End": datetime.strftime(end_time, "%Y-%m-%d"),
                },
                Granularity=self._cost_query_fields["granularity"],
                Metrics=self._cost_query_fields["metrics"],
                Filter={
                    "Dimensions": {
                        "Key": "SERVICE",
                        "Values": self._cost_query_fields["services"],
                    }
                },
                GroupBy=[
                    {"Type": "DIMENSION", "Key": "SERVICE"},
                ],
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

        if self._enable_cost_monitoring:
            # Create a dictionary to store the dataframes
            df_dict = {}

            # Loop over the results for each day
            for result in response_cost["ResultsByTime"]:
                start = datetime.strptime(result["TimePeriod"]["Start"], "%Y-%m-%d")

                # Create a dictionary to store the costs for each service
                cost_dict = {}

                for group in result["Groups"]:
                    for metric in group["Metrics"]:
                        item_id = f"{self.SERVICE_DICT[group['Keys'][0]]}_{metric}"
                        cost_dict[item_id] = float(group["Metrics"][metric]["Amount"])

                # Convert the dictionary to a dataframe and add it to the dictionary
                df_dict[start] = pd.DataFrame(cost_dict, index=[start])

            # Concatenate the dataframes into a single dataframe
            df_temp = pd.concat(df_dict.values(), axis=0).fillna(0)
            df = pd.concat([df, df_temp], axis=1)

        for metric_id in self._metric_ids:
            if metric_id not in df.columns:
                df[metric_id] = pd.Series(dtype=np.float64)

        # Append only the new rows to the internal representation
        self._values = (
            df.copy()
            if self._values.empty
            else pd.concat([self._values, df.loc[df.index > self._values.index[-1]]])
        )
        self._forecaster.update_df_pointer(self._values)
