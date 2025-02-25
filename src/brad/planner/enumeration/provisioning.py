import importlib.resources as pkg_resources
import json
import math

from typing import Iterator, Dict, Tuple, Optional

from brad.blueprint.provisioning import Provisioning
from brad.config.engine import Engine
import brad.planner.scoring.data as scoring_data


class ProvisioningEnumerator:
    """
    Helps with enumerating possible provisionings within some distance of a
    starting provision.

    The distance is designed to be arbitrarily defined, but is currently based
    on the resources available on the provisioning.
    """

    def __init__(self, for_engine: Engine) -> None:
        if for_engine == Engine.Aurora:
            self._instances = _AURORA_INSTANCES
        elif for_engine == Engine.Redshift:
            self._instances = _REDSHIFT_INSTANCES
        else:
            raise ValueError("Unsupported engine: {}".format(for_engine))

    def enumerate_nearby(
        self, base_provisioning: Provisioning, max_distance: float
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
        #  Redshift:
        #   5 instance types x 128 maximum nodes per cluster = 640 configurations.
        #
        #  Aurora:
        #   35 instance types x 15 maximum nodes per cluster = 525 configurations.
        #
        # Thus our strategy is simple: compute the distance between each
        # possible provisioning and the base provisioning. Yield the
        # provisionings that fall within the given maximum distance.
        #
        # We special case a configuration with 0 nodes (represents the engine
        # being shut down).

        base_provisioning_value = self._compute_resource_value(base_provisioning)
        candidate = base_provisioning.mutable_clone()

        # Zero node case.
        candidate.set_num_nodes(0)
        if (
            abs(self._compute_distance(base_provisioning_value, candidate))
            <= max_distance
        ):
            yield candidate

        is_redshift_ra3 = base_provisioning.instance_type().startswith("ra3")

        # Special casing for Redshift dc2. We want to consider additional
        # provisionings that enable elastic resizes.
        is_redshift_dc2 = base_provisioning.instance_type().startswith("dc2")
        double = base_provisioning.num_nodes() * 2
        half = base_provisioning.num_nodes() // 2

        # Consider all other provisionings.
        for instance_type, specs in self._instances.items():
            if is_redshift_ra3 and instance_type.startswith("dc2"):
                # Redshift does not support resizing from ra3 to dc2. This
                # should be encoded as part of our constraints, but for now we
                # leave this here for convenience.
                continue

            candidate.set_instance_type(instance_type)
            for num_nodes in range(
                int(specs["min_nodes"]), int(specs["max_nodes"]) + 1
            ):
                candidate.set_num_nodes(num_nodes)

                if (
                    abs(self._compute_distance(base_provisioning_value, candidate))
                    <= max_distance
                ):
                    yield candidate
                elif is_redshift_dc2 and (
                    num_nodes == double or (half > 0 and num_nodes == half)
                ):
                    yield candidate

    # NOTE: These distance metrics should be taken out of here and abstracted as
    # part of the planner transition score.

    def scaling_to_distance(
        self,
        base_provisioning: Provisioning,
        max_scaling_multiplier: float,
        engine: Engine,
    ) -> float:
        """
        Helps with selecting a maximum distance value based on intuitive scaling
        limits on the existing provisioning.
        """
        assert engine != Engine.Athena

        if base_provisioning.num_nodes() == 0:
            # Special case. Instead of using the current provisioning (which has
            # 0 nodes, i.e., the engine is off), we use the "smallest" possible
            # provisioning instead.
            if engine == Engine.Aurora:
                base_value = float(_MIN_AURORA[1]["resource_value"])
            elif engine == Engine.Redshift:
                base_value = float(_MIN_REDSHIFT[1]["resource_value"])
            else:
                raise RuntimeError("Unsupported engine {}".format(engine))
        else:
            base_value = (
                float(
                    self._instances[base_provisioning.instance_type()]["resource_value"]
                )
                * base_provisioning.num_nodes()
            )

        return base_value * max_scaling_multiplier

    def _compute_distance(self, source_value: float, dest: Provisioning) -> float:
        return self._compute_resource_value(dest) - source_value

    def _compute_resource_value(self, prov: Provisioning) -> float:
        return (
            float(self._instances[prov.instance_type()]["resource_value"])
            * prov.num_nodes()
        )


def _load_instance_resources(file_name: str) -> Dict[str, Dict[str, int | float]]:
    # Load data.
    with pkg_resources.files(scoring_data).joinpath(file_name).open(
        "r", encoding="UTF-8"
    ) as data:
        instances = json.load(data)
    instances_map = {}
    for inst in instances:
        instances_map[inst["instance_type"]] = {
            "vcpus": inst["vcpus"],
            "memory_mib": inst["memory_mib"],
            "resource_value": math.sqrt(inst["vcpus"] * inst["memory_mib"]),
            "min_nodes": inst["min_nodes"],
            "max_nodes": inst["max_nodes"],
        }
    return instances_map


def _find_min_instance(
    instances: Dict[str, Dict[str, int | float]],
) -> Tuple[str, Dict[str, int | float]]:
    min_type: Optional[str] = None
    min_resources: Optional[Dict[str, int | float]] = None

    for instance_type, resources in instances.items():
        if (
            min_resources is None
            # pylint: disable-next=unsubscriptable-object
            or resources["resource_value"] < min_resources["resource_value"]
        ):
            min_type = instance_type
            min_resources = resources

    assert min_type is not None
    assert min_resources is not None
    return min_type, min_resources


_REDSHIFT_INSTANCES = _load_instance_resources("redshift_instances.json")
_AURORA_INSTANCES = _load_instance_resources("aurora_postgresql_instances.json")

_MIN_REDSHIFT = _find_min_instance(_REDSHIFT_INSTANCES)
_MIN_AURORA = _find_min_instance(_AURORA_INSTANCES)
