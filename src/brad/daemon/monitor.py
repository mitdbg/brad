from brad.config.file import ConfigFile
import importlib.resources as pkg_resources
from typing import Dict, List, Tuple
import json
from brad.config.engine import Engine
import brad.daemon as daemon
from datetime import datetime, timedelta
import boto3
import pandas as pd
import asyncio
import numpy as np
from brad.forecasting.constant_forecaster import ConstantForecaster


class Monitor:
    def __init__(self, config: ConfigFile) -> None:
        self._config = config
        self._epoch_length, self._metrics = self._load_monitored_metrics()
        self._client = boto3.client("cloudwatch")
        self._queries = self._create_queries()
        self._values = pd.DataFrame(index=pd.DatetimeIndex([]), columns=self._queries)
        self._forecaster = ConstantForecaster(self._values, self._epoch_length)

    async def run_forever(self) -> None:
        # Flesh out the monitor - maintain running averages of the underlying
        # engines' metrics.
        while True:
            self._add_metrics()
            await asyncio.sleep(60)  # Read every minute

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

    def _load_monitored_metrics(
        self,
    ) -> Tuple[timedelta, Dict[str, Dict[str, List[str]]]]:
        # Load data.
        with pkg_resources.open_text(daemon, "monitored_metrics.json") as data:
            file_contents = json.load(data)

        epoch_length = timedelta(
            weeks=file_contents["epoch_length"]["weeks"],
            days=file_contents["epoch_length"]["days"],
            hours=file_contents["epoch_length"]["hours"],
            minutes=file_contents["epoch_length"]["minutes"],
        )

        metrics_map: Dict[str, Dict[str, List[str]]] = {}
        for f in file_contents["monitored_metrics"]:
            try:
                eng_name = Engine.from_str(f["engine"])
            except ValueError:
                continue

            metrics_map[eng_name] = {}

            for m in f["metrics"]:
                metrics_map[eng_name][m] = f["metrics"][m]

        return epoch_length, metrics_map

    def _create_queries(self):
        # Create the metric data queries
        metric_data_queries = []
        for engine in self._metrics:
            namespace = ""
            dimensions = []
            if engine == Engine.Aurora:
                namespace = "AWS/RDS"
                dimensions = [
                    {"Name": "EngineName", "Value": "aurora-postgresql"},
                ]
            elif engine == Engine.Redshift:
                namespace = "AWS/Redshift"
                dimensions = [
                    {
                        "Name": "ClusterIdentifier",
                        "Value": self._config.redshift_cluster_id,
                    },
                ]
            elif engine == Engine.Athena:
                namespace = "AWS/Athena"
                dimensions = []

            for metric_name, stats_list in self._metrics[engine].items():
                for stat in stats_list:
                    metric_data_query = {
                        "Id": f"{engine}_{metric_name}_{stat}",
                        "MetricStat": {
                            "Metric": {
                                "Namespace": namespace,
                                "MetricName": metric_name,
                                "Dimensions": dimensions,
                            },
                            "Period": int(self._epoch_length.total_seconds()),
                            "Stat": stat,
                        },
                        "ReturnData": True,
                    }
                    metric_data_queries.append(metric_data_query)

        return metric_data_queries

    def _add_metrics(self):
        # Retrieve datapoints
        now = datetime.now()
        end_time = now - (now - datetime.min) % self._epoch_length
        start_time = end_time - 3 * self._epoch_length

        if not self._values.empty:
            start_time = self._values.index[-1]

        response = self._client.get_metric_data(
            MetricDataQueries=self._queries,
            StartTime=start_time,
            EndTime=end_time,
            ScanBy="TimestampAscending",
        )

        # Parse metrics from json response
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

        # Append only the new rows to the internal representation
        self._values = (
            df.copy()
            if self._values.empty
            else pd.concat([self._values, df.loc[df.index > self._values.index[-1]]])
        )
        self._forecaster.update_df_pointer(self._values)
