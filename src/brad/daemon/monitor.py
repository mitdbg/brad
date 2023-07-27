import asyncio
import logging
from itertools import chain
from typing import List, Optional

from brad.blueprint_manager import BlueprintManager
from brad.config.file import ConfigFile
from brad.daemon.messages import MetricsReport
from brad.daemon.metrics_source import MetricsSourceWithForecasting
from brad.daemon.aurora_metrics import AuroraMetrics
from brad.daemon.front_end_metrics import FrontEndMetrics
from brad.daemon.redshift_metrics import RedshiftMetrics

logger = logging.getLogger(__name__)


class Monitor:
    def __init__(
        self,
        config: ConfigFile,
        blueprint_mgr: BlueprintManager,
        forecasting_method: str = "constant",  # {constant, moving_average, linear}
        forecasting_window_size: int = 5,  # (Up to) how many past samples to base the forecast on
    ) -> None:
        self._config = config
        self._blueprint_mgr = blueprint_mgr
        self._epoch_length = self._config.epoch_length

        self._forecasting_method = forecasting_method
        self._forecasting_window_size = forecasting_window_size

        self._aurora_writer_metrics: Optional[AuroraMetrics] = None
        self._aurora_reader_metrics: List[AuroraMetrics] = []
        self._redshift_metrics: Optional[RedshiftMetrics] = None
        self._front_end_metrics = FrontEndMetrics(
            config, forecasting_method, forecasting_window_size
        )

    def set_up_metrics_sources(self) -> None:
        """
        Must be called to initialize the `Monitor`. Run after loading the
        blueprint into the blueprint manager.
        """
        # TODO: Handle blueprint changes gracefully.
        # TODO: No need to create metrics sources if the engine is paused.
        blueprint = self._blueprint_mgr.get_blueprint()

        aurora_prov = blueprint.aurora_provisioning()
        self._aurora_writer_metrics = AuroraMetrics(
            self._config,
            self._blueprint_mgr,
            reader_instance_index=None,
            forecasting_method=self._forecasting_method,
            forecasting_window_size=self._forecasting_window_size,
        )

        if aurora_prov.num_nodes() > 1:
            for reader in range(aurora_prov.num_nodes() - 1):
                self._aurora_reader_metrics.append(
                    AuroraMetrics(
                        self._config,
                        self._blueprint_mgr,
                        reader_instance_index=reader,
                        forecasting_method=self._forecasting_method,
                        forecasting_window_size=self._forecasting_window_size,
                    )
                )

        self._redshift_metrics = RedshiftMetrics(
            self._config, self._forecasting_method, self._forecasting_window_size
        )

    async def fetch_latest(self) -> None:
        """
        Fetches the latest metrics from our metrics sources.
        """
        futures = []
        for source in chain(
            [self._aurora_writer_metrics],
            self._aurora_reader_metrics,
            [self._redshift_metrics],
            [self._front_end_metrics],
        ):
            if source is None:
                continue
            futures.append(source.fetch_latest())
        await asyncio.gather(*futures)

    async def run_forever(self) -> None:
        """
        Periodically fetches the latest metrics.
        """
        while True:
            await self.fetch_latest()
            await asyncio.sleep(self._epoch_length.total_seconds())  # Read every epoch

    def handle_metric_report(self, report: MetricsReport) -> None:
        """
        Used to pass on front-end metrics to the underlying metrics source.
        """
        self._front_end_metrics.handle_metric_report(report)

    # The methods below are used to retrieve metrics.

    def aurora_metrics(
        self, reader_index: Optional[int]
    ) -> MetricsSourceWithForecasting:
        if reader_index is None:
            assert self._aurora_writer_metrics is not None
            return self._aurora_writer_metrics
        else:
            return self._aurora_reader_metrics[reader_index]

    def redshift_metrics(self) -> MetricsSourceWithForecasting:
        assert self._redshift_metrics is not None
        return self._redshift_metrics

    def front_end_metrics(self) -> MetricsSourceWithForecasting:
        assert self._front_end_metrics is not None
        return self._front_end_metrics
