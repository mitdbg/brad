import json
import math
import importlib.resources as pkg_resources
from collections import namedtuple
from typing import Dict, Iterable

import brad.planner.scoring.data as score_data

from brad.blueprint.diff.provisioning import ProvisioningDiff
from brad.blueprint.provisioning import Provisioning
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


def compute_aurora_accessed_pages(
    queries: Iterable[Query],
    accessed_pages_per_query: Iterable[int],
) -> int:
    total_pages = 0
    for query, accessed_pages in zip(queries, accessed_pages_per_query):
        total_pages += query.arrival_count() * accessed_pages
    return total_pages


def compute_aurora_scan_cost(
    total_accessed_pages: int,
    buffer_pool_hit_rate: float,
    planner_config: PlannerConfig,
) -> float:
    expected_ios = (1.0 - buffer_pool_hit_rate) * total_accessed_pages
    expected_million_ios = expected_ios / 1e6
    return expected_million_ios * planner_config.aurora_usd_per_million_ios()


def compute_athena_scanned_bytes(
    queries: Iterable[Query],
    accessed_bytes_per_query: Iterable[int],
    planner_config: PlannerConfig,
) -> int:
    # N.B. There is a minimum charge of 10 MB per query.
    min_bytes_per_query = planner_config.athena_min_mb_per_query() * 1000 * 1000
    total_accessed_bytes = 0
    for query, accessed_bytes in zip(queries, accessed_bytes_per_query):
        total_accessed_bytes += query.arrival_count() * max(
            accessed_bytes, min_bytes_per_query
        )
    return total_accessed_bytes


def compute_athena_scan_cost(
    total_accessed_bytes: int,
    planner_config: PlannerConfig,
) -> float:
    accessed_mb = total_accessed_bytes / 1000 / 1000
    return accessed_mb * planner_config.athena_usd_per_mb_scanned()


def compute_aurora_transition_time_s(
    old: Provisioning, new: Provisioning, planner_config: PlannerConfig
) -> float:
    diff = ProvisioningDiff.of(old, new)
    if diff is None:
        return 0.0

    if diff.new_num_nodes() is not None and diff.new_num_nodes() == 0:
        # Special case: Shutting down an engine is "free".
        return 0.0

    # We transition one instance at a time to minimize disruption.
    num_nodes_to_create = new.num_nodes() - old.num_nodes()

    if new.instance_type() != old.instance_type():
        # We modify "overlapping" nodes. For the primary instance, we actually
        # create a second replica and run a failover, but this is fast enough
        # that we just treat it as one modification.
        num_nodes_to_modify = min(old.num_nodes(), new.num_nodes())
    else:
        # Only adding/removing replicas. We only need to wait when creating
        # replicas.
        num_nodes_to_modify = 0

    return planner_config.aurora_per_instance_change_time_s() * (
        num_nodes_to_modify + num_nodes_to_create
    )


def compute_redshift_transition_time_s(
    old: Provisioning, new: Provisioning, planner_config: PlannerConfig
) -> float:
    diff = ProvisioningDiff.of(old, new)
    if diff is None:
        return 0.0

    if diff.new_num_nodes() is not None and diff.new_num_nodes() == 0:
        # Special case: Shutting down an engine is "free".
        return 0.0

    # Some provisioning changes may take longer than others (classic vs. elastic
    # resize and also the time it takes to transfer data). To start, we use one
    # fixed time.
    return planner_config.redshift_provisioning_change_time_s()


def aurora_resource_value(prov: Provisioning) -> float:
    specs = AuroraSpecs[prov.instance_type()]
    return math.sqrt(specs.vcpus * specs.mem_mib)


def redshift_resource_value(prov: Provisioning) -> float:
    specs = RedshiftSpecs[prov.instance_type()]
    return math.sqrt(specs.vcpus * specs.mem_mib) * prov.num_nodes()


def aurora_num_cpus(prov: Provisioning) -> int:
    specs = AuroraSpecs[prov.instance_type()]
    return specs.vcpus


def redshift_num_cpus(prov: Provisioning) -> int:
    specs = RedshiftSpecs[prov.instance_type()]
    return specs.vcpus
