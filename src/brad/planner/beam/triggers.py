from typing import List

from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.monitor import Monitor
from brad.planner.router_provider import RouterProvider
from brad.planner.scoring.data_access.provider import DataAccessProvider
from brad.planner.triggers.aurora_cpu_utilization import AuroraCpuUtilization
from brad.planner.triggers.redshift_cpu_utilization import RedshiftCpuUtilization
from brad.planner.triggers.elapsed_time import ElapsedTimeTrigger
from brad.planner.triggers.query_latency_ceiling import QueryLatencyCeiling
from brad.planner.triggers.recent_change import RecentChange
from brad.planner.triggers.trigger import Trigger
from brad.planner.triggers.txn_latency_ceiling import TransactionLatencyCeiling
from brad.planner.triggers.variable_costs import VariableCosts


def get_beam_triggers(
    config: ConfigFile,
    planner_config: PlannerConfig,
    monitor: Monitor,
    data_access_provider: DataAccessProvider,
    router_provider: RouterProvider,
) -> List[Trigger]:
    trigger_config = planner_config.trigger_configs()
    if not trigger_config["enabled"]:
        return []

    planning_window = planner_config.planning_window()
    trigger_list: List[Trigger] = []

    et_config = trigger_config["elapsed_time"]
    if "disabled" not in et_config:
        trigger_list.append(
            ElapsedTimeTrigger(
                planning_window * et_config["multiplier"],
                epoch_length=config.epoch_length,
            )
        )

    aurora_cpu = trigger_config["aurora_cpu"]
    if "disabled" not in aurora_cpu:
        trigger_list.append(
            AuroraCpuUtilization(
                monitor, epoch_length=config.epoch_length, **aurora_cpu
            )
        )

    redshift_cpu = trigger_config["redshift_cpu"]
    if "disabled" not in redshift_cpu:
        trigger_list.append(
            RedshiftCpuUtilization(
                monitor, epoch_length=config.epoch_length, **redshift_cpu
            )
        )

    var_costs = trigger_config["variable_costs"]
    if "disabled" not in var_costs:
        trigger_list.append(
            VariableCosts(
                config,
                planner_config,
                monitor,
                data_access_provider,
                router_provider,
                var_costs["threshold"],
                config.epoch_length,
            )
        )

    latency_ceiling = trigger_config["query_latency_ceiling"]
    if "disabled" not in latency_ceiling:
        trigger_list.append(
            QueryLatencyCeiling(
                monitor,
                latency_ceiling["ceiling_s"],
                latency_ceiling["sustained_epochs"],
                config.epoch_length,
            )
        )

    txn_latency_ceiling = trigger_config["txn_latency_ceiling"]
    if "disabled" not in txn_latency_ceiling:
        trigger_list.append(
            TransactionLatencyCeiling(
                monitor,
                latency_ceiling["ceiling_s"],
                latency_ceiling["sustained_epochs"],
                config.epoch_length,
            )
        )

    recent_change = trigger_config["recent_change"]
    if "disabled" not in recent_change:
        trigger_list.append(RecentChange(planner_config, config.epoch_length))

    return trigger_list
