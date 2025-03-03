import asyncio
import logging
from itertools import chain
from typing import List, Optional, Tuple

from brad.blueprint.manager import BlueprintManager
from brad.config.file import ConfigFile
from brad.daemon.messages import MetricsReport, VdbeMetricsReport
from brad.daemon.metrics_source import MetricsSourceWithForecasting
from brad.daemon.aurora_metrics import AuroraMetrics
from brad.daemon.front_end_metrics import FrontEndMetrics
from brad.daemon.redshift_metrics import RedshiftMetrics
from brad.daemon.vdbe_metrics import VdbeMetrics

logger = logging.getLogger(__name__)


class Monitor:
    def __init__(
        self,
        config: ConfigFile,
        blueprint_mgr: BlueprintManager,
        forecasting_method: str = "constant",  # {constant, moving_average, linear}
        forecasting_window_size: int = 5,  # (Up to) how many past samples to base the forecast on
        create_vdbe_metrics: bool = False,
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
        if create_vdbe_metrics:
            self._vdbe_metrics: Optional[VdbeMetrics] = VdbeMetrics(
                config, forecasting_method, forecasting_window_size
            )
        else:
            self._vdbe_metrics = None

    def set_up_metrics_sources(self) -> None:
        """
        Must be called to initialize the `Monitor`. Run after loading the
        blueprint into the blueprint manager.
        """
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
            self._config,
            self._blueprint_mgr,
            self._forecasting_method,
            self._forecasting_window_size,
        )

    def update_metrics_sources(self) -> None:
        """
        Updates the metrics sources when the blueprint changes.
        """
        blueprint = self._blueprint_mgr.get_blueprint()
        num_replicas = blueprint.aurora_provisioning().num_nodes() - 1

        if self._aurora_writer_metrics is not None:
            self._aurora_writer_metrics.update_clients()

        if num_replicas <= 0:
            self._aurora_reader_metrics.clear()
            return

        # Remove any unneeded sources.
        while len(self._aurora_reader_metrics) > num_replicas:
            self._aurora_reader_metrics.pop()

        # Refresh existing sources.
        for source in self._aurora_reader_metrics:
            source.update_clients()

        # Add new sources if needed.
        if len(self._aurora_reader_metrics) < num_replicas:
            next_index = len(self._aurora_reader_metrics)
            while next_index < num_replicas:
                self._aurora_reader_metrics.append(
                    AuroraMetrics(
                        self._config,
                        self._blueprint_mgr,
                        reader_instance_index=next_index,
                        forecasting_method=self._forecasting_method,
                        forecasting_window_size=self._forecasting_window_size,
                    )
                )
                next_index += 1

        # Refresh the Redshift client (we may be switching to/from a preset
        # cluster).
        if self._redshift_metrics is not None:
            self._redshift_metrics.update_clients()

    async def fetch_latest(self) -> None:
        """
        Fetches the latest metrics from our metrics sources.
        """
        logger.debug("Fetching latest metrics...")
        futures = []
        chain_parts = [
            [self._aurora_writer_metrics],
            self._aurora_reader_metrics,
            [self._redshift_metrics],
            [self._front_end_metrics],
        ]
        if self._vdbe_metrics is not None:
            chain_parts.append([self._vdbe_metrics])
        for source in chain(*chain_parts):  # type: ignore
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
            self._print_key_metrics()
            await asyncio.sleep(self._epoch_length.total_seconds())  # Read every epoch

    def handle_metric_report(self, report: MetricsReport) -> None:
        """
        Used to pass on front-end metrics to the underlying metrics source.
        """
        self._front_end_metrics.handle_metric_report(report)

    def handle_vdbe_metric_report(self, report: VdbeMetricsReport) -> None:
        """
        Used to pass on VDBE metrics to the underlying metrics source.
        """
        if self._vdbe_metrics is not None:
            self._vdbe_metrics.handle_metric_report(report)

    def _print_key_metrics(self) -> None:
        # Used for debug purposes.
        if logger.level > logging.DEBUG:
            return

        fe = self.front_end_metrics().read_k_most_recent(2)
        logger.debug("Front end metrics:\n%s", fe)

        redshift = self.redshift_metrics().read_k_most_recent(
            2, ["CPUUtilization_Average", "ReadIOPS_Average"]
        )
        logger.debug("Redshift metrics:\n%s", redshift)

        aurora_metric_names = [
            "os.cpuUtilization.total.avg",
            "os.loadAverageMinute.one.avg",
            "BufferCacheHitRatio_Average",
        ]
        aurora = self.aurora_writer_metrics().read_k_most_recent(2, aurora_metric_names)
        logger.debug("Aurora writer metrics:\n%s", aurora)

        aurora_readers = self.aurora_reader_metrics()
        if len(aurora_readers) > 0:
            aurora_reader_metrics = aurora_readers[0].read_k_most_recent(
                2, aurora_metric_names
            )
            logger.debug("Aurora reader (0) metrics:\n%s", aurora_reader_metrics)

    # The methods below are used to retrieve metrics.

    def aurora_writer_metrics(self) -> MetricsSourceWithForecasting:
        assert self._aurora_writer_metrics is not None
        return self._aurora_writer_metrics

    def aurora_reader_metrics(self) -> Tuple[MetricsSourceWithForecasting, ...]:
        return tuple(self._aurora_reader_metrics)

    def redshift_metrics(self) -> MetricsSourceWithForecasting:
        assert self._redshift_metrics is not None
        return self._redshift_metrics

    def front_end_metrics(self) -> MetricsSourceWithForecasting:
        assert self._front_end_metrics is not None
        return self._front_end_metrics

    def vdbe_metrics(self) -> Optional[MetricsSourceWithForecasting]:
        return self._vdbe_metrics
