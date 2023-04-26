from brad.config.file import ConfigFile
from importlib.resources import files, as_file
from typing import Dict, List, Tuple
import json
from brad.config.engine import Engine
import brad.daemon as daemon
from datetime import datetime, timedelta, timezone
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import time


# Return the id of a metric in the dataframe.
def get_metric_id(engine: str, metric_name: str, stat: str):
    return f"{engine}_{metric_name}_{stat}"


# Monitor
class Monitor:
    # Initialize.
    def __init__(self, cluster_ids: Dict[str, str]) -> None:
        self._cluster_ids = cluster_ids
        self._epoch_length, self._metrics = self._load_monitored_metrics()
        self._client = boto3.client("cloudwatch")
        self._queries = self._create_queries()
        self._values = pd.DataFrame()

    # Create from config file.
    @classmethod
    def from_config_file(cls, config: ConfigFile):
        raise NotImplementedError

    # Create from schema name.
    @classmethod
    def from_schema_name(cls, schema_name: str):
        cluster_ids = {
            Engine.Redshift.lower(): f"brad-{schema_name}",
            Engine.Aurora.lower(): f"brad-{schema_name}",
            Engine.Athena.lower(): f"brad-{schema_name}",
        }
        return cls(cluster_ids)

    # Forcibly fetch metrics. To avoid running long running tests.
    def force_read_metrics(self) -> None:
        self._add_metrics()

    async def run_forever(self) -> None:
        # Flesh out the monitor - maintain running averages of the underlying
        # engines' metrics.
        while True:
            self._add_metrics()
            # Just for testing.
            print(self._values.head())
            time.sleep(300)  # Read every 5 minutes

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
        # TODO(Amadou): Resolve monitor epoch and timezone with Markos. Also discuss what happens when some metrics are missing.
        metrics_file = files(daemon).joinpath("test_monitored_metrics.json")
        with as_file(metrics_file) as file:
            with open(file, "r", encoding="utf8") as data:
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
                    {
                        "Name": "DBClusterIdentifier",
                        "Value": self._cluster_ids[Engine.Aurora],
                    },
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

            for metric_name, stats_list in self._metrics[engine].items():
                for stat in stats_list:
                    metric_data_query = {
                        "Id": get_metric_id(engine.lower(), metric_name, stat),
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
        print(f"Monitoring query: {metric_data_queries}")
        return metric_data_queries

    def _add_metrics(self):
        # Retrieve datapoints
        now = datetime.now(timezone.utc)
        end_time = now  # - (now - datetime.min.replace(tzinfo=now.tzinfo)) % self._epoch_length
        start_time = end_time - 1 * self._epoch_length

        if not self._values.empty:
            start_time = self._values.index[-1]

        try:
            response = self._client.get_metric_data(
                MetricDataQueries=self._queries,
                StartTime=start_time,
                EndTime=end_time,
                ScanBy="TimestampAscending",
            )
            print(f"Monitor response: {response}")
        except ClientError as _e:
            print(f"Cloudwatch metrics error: {_e}")
            return

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
