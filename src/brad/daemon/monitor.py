import asyncio
import boto3
import importlib.resources as pkg_resources
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import brad.daemon as daemon
from brad.config.file import ConfigFile
from brad.config.engine import Engine


class Monitor:
    def __init__(self, config: ConfigFile) -> None:
        self._config = config
        self._epoch_length, self._metrics = self._load_monitored_metrics()
        self._client = boto3.client("cloudwatch")
        self._queries = self._create_queries()
        self._values = pd.DataFrame()

    async def run_forever(self) -> None:
        # Flesh out the monitor - maintain running averages of the underlying
        # engines' metrics.
        while True:
            self._add_metrics()
            await asyncio.sleep(300)  # Read every 5 minutes

    def read_k_most_recent(self, k=1) -> pd.DataFrame | None:
        return None if self._values.empty else self._values.tail(k)

    # Start inclusive, end exclusive
    def read_between(self, start_time, end_time) -> pd.DataFrame | None:
        return (
            None
            if self._values.empty
            else self._values.loc[
                (self._values.index >= start_time) & (self._values.index < end_time)
            ]
        )

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

        # Append only the new rows to the internal representation
        data = {
            result["Id"]: result["Values"] for result in response["MetricDataResults"]
        }
        df = pd.DataFrame(
            data, index=pd.DatetimeIndex(response["MetricDataResults"][0]["Timestamps"])
        )
        df.index = df.index.tz_localize(None)

        self._values = (
            df.copy()
            if self._values.empty
            else pd.concat([self._values, df.loc[df.index > self._values.index[-1]]])
        )
