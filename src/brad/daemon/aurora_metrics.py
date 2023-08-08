import asyncio
import pandas as pd
import json
import pytz
from datetime import timedelta, datetime
from typing import List, Optional, Tuple
from importlib.resources import files, as_file

import brad.daemon as daemon
from .cloudwatch import CloudWatchClient
from .metrics_def import MetricDef
from .metrics_logger import MetricsLogger
from .metrics_source import MetricsSourceWithForecasting
from .perf_insights import PerfInsightsClient
from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.utils.time_periods import impute_old_missing_metrics


class AuroraMetrics(MetricsSourceWithForecasting):
    METRICS_DELAY = timedelta(minutes=1)

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
        self._reader_instance_index = reader_instance_index

        self._pi_metrics, self._cw_metrics = self._load_metric_defs()
        self._values = pd.DataFrame(
            columns=[
                *PerfInsightsClient.metric_names(self._pi_metrics),
                *CloudWatchClient.metric_names(self._cw_metrics),
            ]
        )

        self.update_clients()
        self._logger = MetricsLogger.create_from_config(
            self._config, self._metrics_logger_name(reader_instance_index)
        )

        super().__init__(
            self._config.epoch_length, forecasting_method, forecasting_window_size
        )

    def update_clients(self) -> None:
        directory = self._blueprint_mgr.get_directory()
        resource_id = (
            directory.aurora_writer().resource_id()
            if self._reader_instance_index is None
            else directory.aurora_readers()[self._reader_instance_index].resource_id()
        )
        self._pi_client = PerfInsightsClient(resource_id, self._config)
        if self._reader_instance_index is None:
            self._cw_client = CloudWatchClient(
                Engine.Aurora,
                cluster_identifier=None,
                instance_identifier=directory.aurora_writer().instance_id(),
                config=self._config,
            )
        else:
            self._cw_client = CloudWatchClient(
                Engine.Aurora,
                cluster_identifier=None,
                instance_identifier=directory.aurora_readers()[
                    self._reader_instance_index
                ].instance_id(),
                config=self._config,
            )

    async def fetch_latest(self) -> None:
        loop = asyncio.get_running_loop()
        new_pi_metrics = await loop.run_in_executor(None, self._fetch_pi_metrics, 5)
        new_cw_metrics = await loop.run_in_executor(None, self._fetch_cw_metrics, 5)
        new_metrics = pd.merge(
            new_pi_metrics,
            new_cw_metrics,
            left_index=True,
            right_index=True,
            how="inner",
        )

        # See the comment in `redshift_metrics.py`.
        now = datetime.now().astimezone(pytz.utc)
        cutoff_ts = now - self.METRICS_DELAY
        new_metrics = impute_old_missing_metrics(new_metrics, cutoff_ts, value=0.0)
        new_metrics = new_metrics.dropna()

        self._values = self._get_updated_metrics(new_metrics)
        await super().fetch_latest()

    def real_time_delay(self) -> int:
        # The cache hit rate from CloudWatch can be delayed up to 1 minute.
        num_epochs = self.METRICS_DELAY / self._epoch_length
        return int(num_epochs)  # Want to floor this number.

    def _metrics_values(self) -> pd.DataFrame:
        return self._values

    def _metrics_logger(self) -> Optional[MetricsLogger]:
        return self._logger

    def _metrics_logger_name(self, reader_instance_index: Optional[int]) -> str:
        if reader_instance_index is None:
            return "brad_metrics_aurora_writer.log"
        else:
            return "brad_metrics_aurora_reader_{}.log".format(reader_instance_index)

    def _load_metric_defs(self) -> Tuple[List[MetricDef], List[MetricDef]]:
        metrics_file = files(daemon).joinpath("monitored_aurora_metrics.json")
        with as_file(metrics_file) as file:
            with open(file, "r", encoding="utf8") as data:
                raw_metrics = json.load(data)

        pi_metrics: List[MetricDef] = []
        for metric, stats in raw_metrics["perf_insights"].items():
            for stat in stats:
                pi_metrics.append((metric, stat))

        cw_metrics: List[MetricDef] = []
        for metric, stats in raw_metrics["cloudwatch"].items():
            for stat in stats:
                cw_metrics.append((metric, stat))

        return pi_metrics, cw_metrics

    def _fetch_pi_metrics(self, num_prev_points: int) -> pd.DataFrame:
        return self._pi_client.fetch_metrics(
            self._pi_metrics,
            period=self._config.epoch_length,
            num_prev_points=num_prev_points,
        )

    def _fetch_cw_metrics(self, num_prev_points: int) -> pd.DataFrame:
        return self._cw_client.fetch_metrics(
            self._cw_metrics,
            period=self._config.epoch_length,
            num_prev_points=num_prev_points,
        )
