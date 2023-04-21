from typing import Iterator

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from .blueprint import EnumeratedBlueprint
from .provisioning import ProvisioningEnumerator
from .table_locations import TableLocationEnumerator


class NeighborhoodBlueprintEnumerator:
    """
    Used to enumerate blueprints "nearby" a base blueprint.
    """

    @staticmethod
    def enumerate(
        base_blueprint: Blueprint,
        max_num_table_moves: int,
        max_provisioning_multiplier: float,
    ) -> Iterator[EnumeratedBlueprint]:
        enum_bp = EnumeratedBlueprint(base_blueprint)

        aurora_enum = ProvisioningEnumerator(Engine.Aurora)
        redshift_enum = ProvisioningEnumerator(Engine.Redshift)

        max_aurora_dist = aurora_enum.scaling_to_distance(
            base_blueprint.aurora_provisioning(), max_provisioning_multiplier
        )
        max_redshift_dist = redshift_enum.scaling_to_distance(
            base_blueprint.redshift_provisioning(), max_provisioning_multiplier
        )

        for aurora_prov in aurora_enum.enumerate_nearby(
            base_blueprint.aurora_provisioning(), max_aurora_dist
        ):
            for redshift_prov in redshift_enum.enumerate_nearby(
                base_blueprint.redshift_provisioning(), max_redshift_dist
            ):
                for locations in TableLocationEnumerator.enumerate_nearby(
                    base_blueprint.table_locations(), max_num_table_moves
                ):
                    enum_bp.set_aurora_provisioning(aurora_prov)
                    enum_bp.set_redshift_provisioning(redshift_prov)
                    enum_bp.set_table_locations(locations)
                    yield enum_bp
