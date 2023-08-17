import asyncio
import pandas as pd
import json
import pytz
from datetime import timedelta, datetime
from typing import List, Optional
from importlib.resources import files, as_file

import brad.daemon as daemon
from .metrics_def import MetricDef
from .metrics_source import MetricsSourceWithForecasting
from .metrics_logger import MetricsLogger
from .cloudwatch import CloudWatchClient
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.utils.time_periods import impute_old_missing_metrics


class RedshiftMetrics(MetricsSourceWithForecasting):
    # Indicates that metrics for the last 5 minutes may not be available.
    METRICS_DELAY = timedelta(minutes=5)

    def __init__(
        self,
        config: ConfigFile,
        forecasting_method: str,
        forecasting_window_size: int,
    ) -> None:
        self._config = config
        self._metric_defs = self._load_metric_defs()
        self._values = pd.DataFrame(
            columns=CloudWatchClient.metric_names(self._metric_defs)
        )
        self._cw_client = CloudWatchClient(
            Engine.Redshift,
            self._config.redshift_cluster_id,
            instance_identifier=None,
            config=self._config,
        )
        self._logger = MetricsLogger.create_from_config(
            self._config, "brad_metrics_redshift.log"
        )

        super().__init__(
            self._config.epoch_length, forecasting_method, forecasting_window_size
        )

    async def fetch_latest(self) -> None:
        loop = asyncio.get_running_loop()
        new_metrics = await loop.run_in_executor(None, self._fetch_cw_metrics, 5)

        # CloudWatch has delayed metrics reporting, in particular for CPU
        # utilization (i.e., metrics for the last minute are not always
        # immediately available.). Our metrics client will report `NaN` for
        # missing metrics.
        #
        # The logic below drops rows that contain `NaN` values. To avoid
        # "forever missing" metric values, we set a cutoff of 3 minutes. Any
        # metrics that are older than 3 minutes are presumed to be 0.
        #
        # This approach ensures that clients of this object have reliable access
        # to metrics (i.e., a set of metrics for a period will only appear in
        # the DataFrame once we are confident they are all available).
        now = datetime.now().astimezone(pytz.utc)
        cutoff_ts = now - self.METRICS_DELAY
        new_metrics = impute_old_missing_metrics(new_metrics, cutoff_ts, value=0.0)
        new_metrics = new_metrics.dropna()

        self._values = self._get_updated_metrics(new_metrics)
        await super().fetch_latest()

    def real_time_delay(self) -> int:
        # Usually, Redshift metrics are delayed up to 3 minutes.
        num_epochs = self.METRICS_DELAY / self._epoch_length
        return int(num_epochs)  # Want to floor this number.

    def _metrics_values(self) -> pd.DataFrame:
        return self._values

    def _metrics_logger(self) -> Optional[MetricsLogger]:
        return self._logger

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
