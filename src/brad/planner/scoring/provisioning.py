import json
import math
import importlib.resources as pkg_resources
import numpy as np
import numpy.typing as npt
from collections import namedtuple
from typing import Dict, Iterable, TYPE_CHECKING

import brad.planner.scoring.data as score_data

from brad.blueprint.diff.provisioning import ProvisioningDiff
from brad.blueprint.provisioning import Provisioning
from brad.config.planner import PlannerConfig
from brad.planner.workload.query import Query
from brad.provisioning.redshift import RedshiftProvisioningManager

if TYPE_CHECKING:
    from brad.planner.scoring.context import ScoringContext


ProvisioningResources = namedtuple(
    "ProvisioningResources",
    ["instance_type", "usd_per_hour", "vcpus", "mem_mib", "io_opt_usd_per_hour"],
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
            config["io_opt_usd_per_hour"] if "io_opt_usd_per_hour" in config else None,
        )
        for config in raw_json
    }


AuroraSpecs = _load_instance_specs("aurora_postgresql_instances.json")
RedshiftSpecs = _load_instance_specs("redshift_instances.json")


def compute_aurora_hourly_operational_cost(
    provisioning: Provisioning, ctx: "ScoringContext"
) -> float:
    prov = AuroraSpecs[provisioning.instance_type()]
    if ctx.planner_config.use_io_optimized_aurora():
        hourly_cost = prov.io_opt_usd_per_hour
    else:
        hourly_cost = prov.usd_per_hour
    return hourly_cost * provisioning.num_nodes()


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
    arrival_counts = 0.0
    for query, accessed_pages in zip(queries, accessed_pages_per_query):
        total_pages += accessed_pages
        arrival_counts += query.arrival_count()
    return max(int(total_pages * arrival_counts), 1)


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
    return compute_athena_scanned_bytes_batch(
        np.array(accessed_bytes_per_query),
        np.array(map(lambda query: query.arrival_count(), queries)),
        planner_config,
    )


def compute_athena_scanned_bytes_batch(
    accessed_bytes_per_query: npt.NDArray,
    arrival_counts: npt.NDArray,
    planner_config: PlannerConfig,
) -> int:
    # N.B. There is a minimum charge of 10 MB per query.
    min_bytes_per_query = planner_config.athena_min_mb_per_query() * 1000 * 1000
    accessed_bytes_pq = np.clip(
        accessed_bytes_per_query, a_min=min_bytes_per_query, a_max=None
    )
    total_bytes = np.dot(accessed_bytes_pq, arrival_counts)
    return max(int(total_bytes.item()), 1)


def compute_athena_scan_cost(
    total_accessed_bytes: int,
    planner_config: PlannerConfig,
) -> float:
    accessed_mb = total_accessed_bytes / 1000 / 1000
    return accessed_mb * planner_config.athena_usd_per_mb_scanned()


def compute_athena_scan_cost_numpy(
    bytes_accessed: npt.NDArray,
    arrival_counts: npt.NDArray,
    planner_config: PlannerConfig,
) -> float:
    # Note we use MB instead of MiB
    mb_accessed = bytes_accessed / 1000.0 / 1000.0
    mb_accessed = np.clip(
        mb_accessed, a_min=float(planner_config.athena_min_mb_per_query()), a_max=None
    )
    total_mb = np.dot(mb_accessed, arrival_counts)
    return total_mb * planner_config.athena_usd_per_mb_scanned()


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
    num_nodes_to_create = max(new.num_nodes() - old.num_nodes(), 0)

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

    if old.num_nodes() == 0 and new.num_nodes() > 0:
        # Special case: Starting a new cluster. We discourage the use of
        # single-node clusters because it prevents the use of elastic resize
        # later on. We treat 0 -> 1 as a "classic resize".
        new_nodes = new.num_nodes()
        if new_nodes == 1:
            return planner_config.redshift_classic_resize_time_s()
        else:
            return planner_config.redshift_elastic_resize_time_s()

    # Some provisioning changes may take longer than others (classic vs. elastic
    # resize and also the time it takes to transfer data).
    must_use_classic = RedshiftProvisioningManager.must_use_classic_resize(old, new)

    if must_use_classic:
        return planner_config.redshift_classic_resize_time_s()
    else:
        return planner_config.redshift_elastic_resize_time_s()


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
