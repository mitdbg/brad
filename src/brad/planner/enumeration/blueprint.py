from typing import Dict, List, Optional

from brad.blueprint import Blueprint
from brad.blueprint.provisioning import Provisioning
from brad.config.engine import Engine


class EnumeratedBlueprint(Blueprint):
    """
    A version of `Blueprint` used during enumeration.

    We want to avoid creating many short-lived `Blueprint`s during enumeration.
    This wrapper class allows the enumerated parts of the blueprint to be
    modified in place.
    """

    def __init__(self, base_blueprint: Blueprint) -> None:
        super().__init__(
            base_blueprint.schema_name(),
            base_blueprint.tables(),
            base_blueprint.table_locations(),
            base_blueprint.aurora_provisioning(),
            base_blueprint.redshift_provisioning(),
            base_blueprint.router_provider(),
        )
        self._current_locations = base_blueprint.table_locations()
        self._current_aurora_provisioning = base_blueprint.aurora_provisioning()
        self._current_redshift_provisioning = base_blueprint.redshift_provisioning()
        self._current_table_locations_bitmap: Optional[Dict[str, int]] = None

    def set_table_locations(
        self, locations: Dict[str, List[Engine]]
    ) -> "EnumeratedBlueprint":
        self._current_locations = locations
        self._current_table_locations_bitmap = None
        return self

    def set_aurora_provisioning(self, prov: Provisioning) -> "EnumeratedBlueprint":
        self._current_aurora_provisioning = prov
        return self

    def set_redshift_provisioning(self, prov: Provisioning) -> "EnumeratedBlueprint":
        self._current_redshift_provisioning = prov
        return self

    def to_blueprint(self) -> Blueprint:
        """
        Makes a copy of this object as a `Blueprint`.
        """

        return Blueprint(
            self.schema_name(),
            self.tables(),
            table_locations={
                name: locations.copy()
                for name, locations in self._current_locations.items()
            },
            aurora_provisioning=self._current_aurora_provisioning.clone(),
            redshift_provisioning=self._current_redshift_provisioning.clone(),
            router_provider=self.router_provider(),
        )

    # Overridden getters.

    def table_locations(self) -> Dict[str, List[Engine]]:
        return self._current_locations

    def table_locations_bitmap(self) -> Dict[str, int]:
        if self._current_table_locations_bitmap is None:
            self._current_table_locations_bitmap = {
                tbl: Engine.to_bitmap(locs)
                for tbl, locs in self._current_locations.items()
            }
        return self._current_table_locations_bitmap

    def aurora_provisioning(self) -> Provisioning:
        return self._current_aurora_provisioning

    def redshift_provisioning(self) -> Provisioning:
        return self._current_redshift_provisioning

    def get_table_locations(self, table_name: str) -> List[Engine]:
        try:
            return self._current_locations[table_name]
        except KeyError as ex:
            raise ValueError from ex
