import asyncio
import pandas as pd
import json
from typing import List
from importlib.resources import files, as_file

import brad.daemon as daemon
from .metrics_def import MetricDef
from .metrics_source import MetricsSourceWithForecasting
from .cloudwatch import CloudwatchClient
from brad.config.engine import Engine
from brad.config.file import ConfigFile


class RedshiftMetrics(MetricsSourceWithForecasting):
    def __init__(
        self,
        config: ConfigFile,
        forecasting_method: str,
        forecasting_window_size: int,
    ) -> None:
        self._config = config
        self._metric_defs = self._load_metric_defs()
        self._values = pd.DataFrame(
            columns=CloudwatchClient.metric_names(self._metric_defs)
        )
        self._cw_client = CloudwatchClient(
            Engine.Redshift, self._config.redshift_cluster_id, self._config
        )

        super().__init__(
            self._config.epoch_length, forecasting_method, forecasting_window_size
        )

    async def fetch_latest(self) -> None:
        loop = asyncio.get_running_loop()
        new_metrics = await loop.run_in_executor(None, self._fetch_cw_metrics, 5)
        self._values = (
            self._values
            if self._values.empty
            else pd.concat(
                [
                    self._values,
                    new_metrics.loc[new_metrics.index > self._values.index[-1]],
                ]
            )
        )
        await super().fetch_latest()

    def _metrics_values(self) -> pd.DataFrame:
        return self._values

    def _load_metric_defs(self) -> List[MetricDef]:
        metrics_file = files(daemon).joinpath("monitored_redshift_metrics.json")
        with as_file(metrics_file) as file:
            with open(file, "r", encoding="utf8") as data:
                raw_metrics = json.load(data)

        metrics: List[MetricDef] = []
        for metric, stats in raw_metrics.items():
            for stat in stats:
                metrics.append((metric, stat))

        return metrics

    def _fetch_cw_metrics(self, num_prev_points: int) -> pd.DataFrame:
        return self._cw_client.fetch_metrics(
            self._metric_defs,
            period=self._config.epoch_length,
            num_prev_points=num_prev_points,
        )