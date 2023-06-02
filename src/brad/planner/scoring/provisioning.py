import json
import importlib.resources as pkg_resources
from collections import namedtuple
from typing import Dict, Iterable

import brad.planner.scoring.data as score_data

from brad.blueprint.diff.provisioning import ProvisioningDiff
from brad.blueprint.provisioning import Provisioning
from brad.config.engine import Engine
from brad.config.planner import PlannerConfig
from brad.planner.workload.query import Query


ProvisioningResources = namedtuple(
    "ProvisioningResources", ["instance_type", "usd_per_hour", "vcpus", "mem_mib"]
)


def _load_instance_specs(file_name: str) -> Dict[str, ProvisioningResources]:
    with pkg_resources.files(score_data).joinpath(file_name).open("r") as data:
        raw_json = json.load(data)

    return {
        config["instance_type"]: ProvisioningResources(
            config["instance_type"],
            config["usd_per_hour"],
            config["vcpus"],
            config["memory_mib"],
        )
        for config in raw_json
    }


AuroraSpecs = _load_instance_specs("aurora_postgresql_instances.json")
RedshiftSpecs = _load_instance_specs("redshift_instances.json")


def compute_aurora_hourly_operational_cost(provisioning: Provisioning) -> float:
    return (
        AuroraSpecs[provisioning.instance_type()].usd_per_hour
        * provisioning.num_nodes()
    )


def compute_redshift_hourly_operational_cost(provisioning: Provisioning) -> float:
    return (
        RedshiftSpecs[provisioning.instance_type()].usd_per_hour
        * provisioning.num_nodes()
    )


def compute_aurora_scan_cost(
    aurora_queries: Iterable[Query],
    planner_config: PlannerConfig,
) -> float:
    # Data access (scan) costs.
    aurora_access_mb = 0
    for q in aurora_queries:
        aurora_access_mb += q.data_accessed_mb(Engine.Aurora)
    return aurora_access_mb * planner_config.aurora_usd_per_mb_scanned()


def compute_athena_scan_cost(
    athena_queries: Iterable[Query],
    planner_config: PlannerConfig,
) -> float:
    athena_access_mb = 0
    for q in athena_queries:
        athena_access_mb += q.data_accessed_mb(Engine.Athena)
    return athena_access_mb * planner_config.athena_usd_per_mb_scanned()


def compute_aurora_transition_time_s(
    old: Provisioning, new: Provisioning, planner_config: PlannerConfig
) -> float:
    diff = ProvisioningDiff.of(old, new)
    if diff is None:
        return 0.0

    # Some provisioning changes may take longer than others. To start, we use
    # one fixed time.
    return planner_config.aurora_provisioning_change_time_s()


def compute_redshift_transition_time_s(
    old: Provisioning, new: Provisioning, planner_config: PlannerConfig
) -> float:
    diff = ProvisioningDiff.of(old, new)
    if diff is None:
        return 0.0

    # Some provisioning changes may take longer than others (classic vs. elastic
    # resize and also the time it takes to transfer data). To start, we use one
    # fixed time.
    return planner_config.redshift_provisioning_change_time_s()
