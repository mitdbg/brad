import asyncio
import pandas as pd
import json
import logging
from datetime import timedelta
from typing import List, Optional, Tuple
from importlib.resources import files, as_file

import brad.daemon as daemon
from .metrics_def import MetricDef
from .metrics_source import MetricsSourceWithForecasting
from .metrics_logger import MetricsLogger
from .cloudwatch import CloudWatchClient, MAX_REDSHIFT_NODES
from brad.blueprint.manager import BlueprintManager
from brad.blueprint.provisioning import Provisioning
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.utils.time_periods import impute_old_missing_metrics, universal_now

logger = logging.getLogger(__name__)


class RedshiftMetrics(MetricsSourceWithForecasting):
    # Indicates that metrics for the last 5 minutes may not be available.
    METRICS_DELAY = timedelta(minutes=5)

    def __init__(
        self,
        config: ConfigFile,
        blueprint_mgr: BlueprintManager,
        forecasting_method: str,
        forecasting_window_size: int,
    ) -> None:
        self._config = config
        self._in_stub_mode = self._config.stub_mode_path is not None
        self._blueprint_mgr = blueprint_mgr
        self._metric_defs = self._load_metric_defs()
        self._values = pd.DataFrame(
            columns=CloudWatchClient.metric_names(self._metric_defs)
        )
        self.update_clients()
        if not self._in_stub_mode:
            self._logger = MetricsLogger.create_from_config(
                self._config, "brad_metrics_redshift.log"
            )
        else:
            self._logger = None

        super().__init__(
            self._config.epoch_length, forecasting_method, forecasting_window_size
        )

    def update_clients(self) -> None:
        if self._in_stub_mode:
            return

        overriden_id = (
            self._blueprint_mgr.get_directory().get_override_redshift_cluster_id()
        )
        redshift_cluster_id = (
            overriden_id
            if overriden_id is not None
            else self._config.redshift_cluster_id
        )
        logger.info("Will fetch Redshift metrics from %s", redshift_cluster_id)
        self._cw_client = CloudWatchClient(
            Engine.Redshift,
            redshift_cluster_id,
            instance_identifier=None,
            config=self._config,
        )

    async def fetch_latest(self) -> None:
        if self._in_stub_mode:
            return

        loop = asyncio.get_running_loop()
        new_metrics = await loop.run_in_executor(None, self._fetch_cw_metrics, 8)

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
        now = universal_now()
        cutoff_ts = now - self.METRICS_DELAY
        new_metrics = impute_old_missing_metrics(new_metrics, cutoff_ts, value=0.0)

        # Some dimension metrics are not relevant for the current blueprint.
        # Note - one potential issue is zeroing out delayed metrics associated
        # with an old blueprint. As long as we have a sufficient delay before
        # allowing another blueprint transition, we should be OK.
        blueprint = self._blueprint_mgr.get_blueprint()
        _, redshift_dimensions_to_discard = relevant_redshift_node_dimensions(
            blueprint.redshift_provisioning()
        )
        to_discard = []
        for metric, stat in self._metric_defs:
            if metric != "CPUUtilization":
                continue
            for dimension in redshift_dimensions_to_discard:
                to_discard.append(f"{metric}_{stat}_{dimension}")
        new_metrics[to_discard] = new_metrics[to_discard].fillna(0.0)

        # Discard any remaining rows that contain NaNs.
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

    @staticmethod
    def _load_metric_defs() -> List[MetricDef]:
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


def relevant_redshift_node_dimensions(
    redshift: Provisioning,
) -> Tuple[List[str], List[str]]:
    """
    Returns the metrics to expect and to discard for the current blueprint.
    """
    dim_to_expect = []
    dim_to_discard = []
    num_nodes = redshift.num_nodes()
    if num_nodes == 1:
        dim_to_expect.append("Shared")
        dim_to_discard.append("Leader")
        for idx in range(MAX_REDSHIFT_NODES):
            dim_to_discard.append(f"Compute{idx}")
    else:
        dim_to_expect.append("Leader")
        dim_to_discard.append("Shared")
        for idx in range(MAX_REDSHIFT_NODES):
            if idx < num_nodes:
                dim_to_expect.append(f"Compute{idx}")
            else:
                dim_to_discard.append(f"Compute{idx}")
    return dim_to_expect, dim_to_discard
