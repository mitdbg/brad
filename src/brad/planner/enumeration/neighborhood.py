from itertools import product
from typing import Iterator

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from .blueprint import EnumeratedBlueprint
from .provisioning import ProvisioningEnumerator
from .table_locations import TableLocationEnumerator


class NeighborhoodBlueprintEnumerator:
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

        aurora_iter = aurora_enum.enumerate_nearby(
            base_blueprint.aurora_provisioning(), max_aurora_dist
        )
        redshift_iter = redshift_enum.enumerate_nearby(
            base_blueprint.redshift_provisioning(), max_redshift_dist
        )
        table_iter = TableLocationEnumerator.enumerate_nearby(
            base_blueprint.table_locations(), max_num_table_moves
        )

        for aurora_prov, redshift_prov, locations in product(
            aurora_iter, redshift_iter, table_iter
        ):
            enum_bp.set_aurora_provisioning(aurora_prov)
            enum_bp.set_redshift_provisioning(redshift_prov)
            enum_bp.set_table_locations(locations)
            yield enum_bp
