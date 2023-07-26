import asyncio
import pandas as pd
import json
from typing import List, Optional
from importlib.resources import files, as_file

import brad.daemon as daemon
from .metrics_def import MetricDef
from .metrics_source import MetricsSourceWithForecasting
from .perf_insights import PerfInsightsClient
from brad.blueprint_manager import BlueprintManager
from brad.config.file import ConfigFile


class AuroraMetrics(MetricsSourceWithForecasting):
    def __init__(
        self,
        config: ConfigFile,
        blueprint_mgr: BlueprintManager,
        reader_instance_index: Optional[int],
        forecasting_method: str,
        forecasting_window_size: int,
    ) -> None:
        self._config = config
        self._blueprint_mgr = blueprint_mgr

        self._metric_defs = self._load_metric_defs()
        self._values = pd.DataFrame(
            columns=PerfInsightsClient.metric_names(self._metric_defs)
        )

        # TODO: This metrics engine needs to be adjusted on blueprint changes.
        directory = blueprint_mgr.get_directory()
        resource_id = (
            directory.aurora_writer().resource_id()
            if reader_instance_index is None
            else directory.aurora_readers()[reader_instance_index].resource_id()
        )
        self._pi_client = PerfInsightsClient(resource_id, config)

        super().__init__(
            self._config.epoch_length, forecasting_method, forecasting_window_size
        )

    async def fetch_latest(self) -> None:
        loop = asyncio.get_running_loop()
        new_metrics = await loop.run_in_executor(None, self._fetch_pi_metrics, 5)
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
        metrics_file = files(daemon).joinpath("monitored_aurora_metrics.json")
        with as_file(metrics_file) as file:
            with open(file, "r", encoding="utf8") as data:
                raw_metrics = json.load(data)

        metrics: List[MetricDef] = []
        for metric, stats in raw_metrics.items():
            for stat in stats:
                metrics.append((metric, stat))

        return metrics

    def _fetch_pi_metrics(self, num_prev_points: int) -> pd.DataFrame:
        return self._pi_client.fetch_metrics(
            self._metric_defs,
            period=self._config.epoch_length,
            num_prev_points=num_prev_points,
        )
