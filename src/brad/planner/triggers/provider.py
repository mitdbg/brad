import logging
from datetime import datetime, timedelta
from typing import List

from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.monitor import Monitor
from brad.planner.estimator import EstimatorProvider
from brad.planner.scoring.data_access.provider import DataAccessProvider
from brad.planner.triggers.aurora_cpu_utilization import AuroraCpuUtilization
from brad.planner.triggers.redshift_cpu_utilization import RedshiftCpuUtilization
from brad.planner.triggers.elapsed_time import ElapsedTimeTrigger
from brad.planner.triggers.query_latency_ceiling import QueryLatencyCeiling
from brad.planner.triggers.recent_change import RecentChange
from brad.planner.triggers.trigger import Trigger
from brad.planner.triggers.txn_latency_ceiling import TransactionLatencyCeiling
from brad.planner.triggers.variable_costs import VariableCosts

logger = logging.getLogger(__name__)


class TriggerProvider:
    """
    Used to customize the triggers that are passed into a blueprint planner.
    """

    def get_triggers(self) -> List[Trigger]:
        raise NotImplementedError


class EmptyTriggerProvider(TriggerProvider):
    def get_triggers(self) -> List[Trigger]:
        return []


class ConfigDefinedTriggers(TriggerProvider):
    def __init__(
        self,
        config: ConfigFile,
        planner_config: PlannerConfig,
        monitor: Monitor,
        data_access_provider: DataAccessProvider,
        estimator_provider: EstimatorProvider,
        startup_timestamp: datetime,
    ) -> None:
        self._config = config
        self._planner_config = planner_config
        self._monitor = monitor
        self._data_access_provider = data_access_provider
        self._estimator_provider = estimator_provider
        self._startup_timestamp = startup_timestamp

    def get_triggers(self) -> List[Trigger]:
        if self._config.stub_mode_path() is not None:
            logger.info("Stub mode enabled - not creating any planner triggers.")
            return []

        trigger_config = self._planner_config.trigger_configs()
        if not trigger_config["enabled"]:
            return []

        planning_window = self._planner_config.planning_window()
        observe_bp_delay = timedelta(
            minutes=trigger_config["observe_new_blueprint_mins"]
        )
        trigger_list: List[Trigger] = []

        et_config = trigger_config["elapsed_time"]
        if "disabled" not in et_config:
            trigger_list.append(
                ElapsedTimeTrigger(
                    planning_window * et_config["multiplier"],
                    epoch_length=self._config.epoch_length,
                    observe_bp_delay=observe_bp_delay,
                )
            )

        aurora_cpu = trigger_config["aurora_cpu"]
        if "disabled" not in aurora_cpu:
            trigger_list.append(
                AuroraCpuUtilization(
                    self._monitor,
                    epoch_length=self._config.epoch_length,
                    observe_bp_delay=observe_bp_delay,
                    **aurora_cpu
                )
            )

        redshift_cpu = trigger_config["redshift_cpu"]
        if "disabled" not in redshift_cpu:
            trigger_list.append(
                RedshiftCpuUtilization(
                    self._monitor,
                    epoch_length=self._config.epoch_length,
                    observe_bp_delay=observe_bp_delay,
                    **redshift_cpu
                )
            )

        var_costs = trigger_config["variable_costs"]
        if "disabled" not in var_costs:
            trigger_list.append(
                VariableCosts(
                    self._config,
                    self._planner_config,
                    self._monitor,
                    self._data_access_provider,
                    self._estimator_provider,
                    var_costs["threshold"],
                    self._config.epoch_length,
                    self._startup_timestamp,
                    observe_bp_delay,
                )
            )

        latency_ceiling = trigger_config["query_latency_ceiling"]
        if "disabled" not in latency_ceiling:
            trigger_list.append(
                QueryLatencyCeiling(
                    self._monitor,
                    latency_ceiling["ceiling_s"],
                    latency_ceiling["sustained_epochs"],
                    self._config.epoch_length,
                    observe_bp_delay=observe_bp_delay,
                )
            )

        txn_latency_ceiling = trigger_config["txn_latency_ceiling"]
        if "disabled" not in txn_latency_ceiling:
            trigger_list.append(
                TransactionLatencyCeiling(
                    self._monitor,
                    latency_ceiling["ceiling_s"],
                    latency_ceiling["sustained_epochs"],
                    self._config.epoch_length,
                    observe_bp_delay=observe_bp_delay,
                )
            )

        recent_change = trigger_config["recent_change"]
        if "disabled" not in recent_change:
            trigger_list.append(
                RecentChange(
                    self._planner_config,
                    self._config.epoch_length,
                    recent_change["delay_epochs"],
                    observe_bp_delay=observe_bp_delay,
                )
            )

        return trigger_list
