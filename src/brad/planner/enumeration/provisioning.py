import importlib.resources as pkg_resources
import json
import math

from typing import Iterator, Dict
from brad.blueprint.provisioning import Provisioning

import brad.planner.scoring.data as scoring_data


class RedshiftProvisioningEnumerator:
    """
    Helps with enumerating possible Redshift provisionings within some distance
    of a starting provision.

    The distance is designed to be arbitrarily defined, but is currently based
    on the resources available on the provisioning.
    """

    @classmethod
    def enumerate_nearby(
        cls, base_provisioning: Provisioning, max_distance: float
    ) -> Iterator[Provisioning]:
        """
        Enumerates provisionings within `max_distance` of the given
        `base_provisioning`.

        The provisionings emitted by this enumerator should not be "stored",
        because the physical object is re-used during enumeration to avoid
        creating many short-lived objects. To build a new blueprint from an
        emitted provisioning, `clone()` the returned object.
        """

        # The number of valid provisionings is small.
        #
        #   5 instance types x 128 maximum nodes per cluster = 640 configurations.
        #
        # Thus our strategy is simple: compute the distance between each
        # possible provisioning and the base provisioning. Yield the
        # provisionings that fall within the given maximum distance.
        #
        # We special case a configuration with 0 nodes (represents Redshift
        # being shut down).

        base_provisioning_value = cls._compute_resource_value(base_provisioning)
        candidate = base_provisioning.mutable_clone()

        # Zero node case.
        candidate.set_num_nodes(0)
        if (
            abs(cls._compute_distance(base_provisioning_value, candidate))
            <= max_distance
        ):
            yield candidate

        # Consider all other provisionings.
        for instance_type, specs in _INSTANCES.items():
            candidate.set_instance_type(instance_type)
            for num_nodes in range(
                int(specs["min_nodes"]), int(specs["max_nodes"]) + 1
            ):
                candidate.set_num_nodes(num_nodes)

                if (
                    abs(cls._compute_distance(base_provisioning_value, candidate))
                    <= max_distance
                ):
                    yield candidate

    # NOTE: These distance metrics should be taken out of here and abstracted as
    # part of the planner transition score.

    @staticmethod
    def scaling_to_distance(
        base_provisioning: Provisioning, max_scaling_multiplier: float
    ) -> float:
        """
        Helps with selecting a maximum distance value based on intuitive scaling
        limits on the existing provisioning.
        """
        return (
            float(_INSTANCES[base_provisioning.instance_type()]["resource_value"])
            * base_provisioning.num_nodes()
            * max_scaling_multiplier
        )

    @classmethod
    def _compute_distance(cls, source_value: float, dest: Provisioning) -> float:
        return cls._compute_resource_value(dest) - source_value

    @staticmethod
    def _compute_resource_value(prov: Provisioning) -> float:
        return (
            float(_INSTANCES[prov.instance_type()]["resource_value"]) * prov.num_nodes()
        )


def _load_instance_resources() -> Dict[str, Dict[str, int | float]]:
    # Load data.
    with pkg_resources.open_text(scoring_data, "redshift_instances.json") as data:
        instances = json.load(data)
    instances_map = {}
    for inst in instances:
        instances_map[inst["instance_type"]] = {
            "vcpus": inst["vcpus"],
            "memory_mib": inst["memory_mib"],
            "resource_value": math.sqrt(inst["vcpus"] * inst["memory_mib"]),
        }
    return instances_map


_INSTANCES = _load_instance_resources()
