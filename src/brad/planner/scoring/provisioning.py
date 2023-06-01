import json
import importlib.resources as pkg_resources
from collections import namedtuple
from typing import Dict

import brad.planner.scoring.data as score_data
from brad.blueprint.provisioning import Provisioning


ProvisioningResources = namedtuple(
    "_Provisioning", ["instance_type", "usd_per_hour", "vcpus", "mem_mib"]
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


def aurora_hourly_operational_cost(provisioning: Provisioning) -> float:
    return (
        AuroraSpecs[provisioning.instance_type()].usd_per_hour
        * provisioning.num_nodes()
    )


def redshift_hourly_operational_cost(provisioning: Provisioning) -> float:
    return (
        RedshiftSpecs[provisioning.instance_type()].usd_per_hour
        * provisioning.num_nodes()
    )
