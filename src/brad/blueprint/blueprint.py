from typing import Dict, List, Set, Tuple, Any

from brad.blueprint.provisioning import Provisioning
from brad.blueprint.table import Table
from brad.config.engine import Engine
from brad.routing.abstract_policy import FullRoutingPolicy


class Blueprint:
    def __init__(
        self,
        schema_name: str,
        table_schemas: List[Table],
        table_locations: Dict[str, List[Engine]],
        aurora_provisioning: Provisioning,
        redshift_provisioning: Provisioning,
        full_routing_policy: FullRoutingPolicy,
    ):
        self._schema_name = schema_name
        self._table_schemas = table_schemas
        self._table_locations = table_locations
        self._aurora_provisioning = aurora_provisioning
        self._redshift_provisioning = redshift_provisioning
        self._full_routing_policy = full_routing_policy

        # Derived properties used for the class' methods.
        self._tables_by_name = {tbl.name: tbl for tbl in self._table_schemas}
        self._base_table_names = self._compute_base_tables()

        self._table_locations_bitmap: Dict[str, int] = {
            tbl: Engine.to_bitmap(locs) for tbl, locs in self._table_locations.items()
        }

    def schema_name(self) -> str:
        return self._schema_name

    def tables(self) -> List[Table]:
        return self._table_schemas

    def table_locations(self) -> Dict[str, List[Engine]]:
        return self._table_locations

    def table_locations_bitmap(self) -> Dict[str, int]:
        return self._table_locations_bitmap

    def tables_with_locations(self) -> List[Tuple[Table, List[Engine]]]:
        result = []
        for table_schema in self._table_schemas:
            result.append((table_schema, self.get_table_locations(table_schema.name)))
        return result

    def aurora_provisioning(self) -> Provisioning:
        return self._aurora_provisioning

    def redshift_provisioning(self) -> Provisioning:
        return self._redshift_provisioning

    def get_routing_policy(self) -> FullRoutingPolicy:
        return self._full_routing_policy

    def base_table_names(self) -> Set[str]:
        return self._base_table_names

    def get_table(self, table_name: str) -> Table:
        try:
            return self._tables_by_name[table_name]
        except KeyError as ex:
            raise ValueError from ex

    def get_table_locations(self, table_name: str) -> List[Engine]:
        try:
            return self._table_locations[table_name]
        except KeyError as ex:
            raise ValueError from ex

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Blueprint):
            return False
        return (
            self.schema_name() == other.schema_name()
            and self.tables() == other.tables()
            and self.table_locations() == other.table_locations()
            and self.aurora_provisioning() == other.aurora_provisioning()
            and self.redshift_provisioning() == other.redshift_provisioning()
            and self.get_routing_policy() == other.get_routing_policy()
        )

    def _compute_base_tables(self) -> Set[str]:
        """
        Compute the base tables in the dependency graph. These are the tables
        with no dependencies.
        """
        visited: Set[str] = set()
        base_tables: Set[str] = set()

        # Recursive depth-first traversal.
        def visit_table(table: Table) -> None:
            if table.name in visited:
                return

            visited.add(table.name)

            if len(table.table_dependencies) == 0:
                base_tables.add(table.name)
                return

            for dep_table_name in table.table_dependencies:
                visit_table(self.get_table(dep_table_name))

        for table in self._table_schemas:
            visit_table(table)

        return base_tables

    def __repr__(self) -> str:
        # Useful for debugging purposes.
        aurora = "Aurora:    " + str(self.aurora_provisioning())
        redshift = "Redshift:  " + str(self.redshift_provisioning())
        tables = "\n  ".join(
            map(
                lambda name_loc: "".join([name_loc[0], ": ", str(name_loc[1])]),
                self.table_locations().items(),
            )
        )
        routing_policy = self.get_routing_policy()
        indef_policy_string = "\n    - ".join(
            [str(policy) for policy in routing_policy.indefinite_policies]
        )
        indefinite_policies = f"Indefinite routing policies:  {indef_policy_string}"
        definite_policy = (
            f"Definite routing policy:      {routing_policy.definite_policy}"
        )
        return "\n  ".join(
            [
                "Blueprint:",
                tables,
                "---",
                aurora,
                redshift,
                "---",
                indefinite_policies,
                definite_policy,
                "---",
                f"Schema name: {self.schema_name()}",
            ]
        )

    def as_dict(self) -> Dict[str, Any]:
        """
        Useful for logging and debugging purposes.
        """
        provisioning = {
            "aurora_instance_type": self.aurora_provisioning().instance_type(),
            "aurora_num_nodes": self.aurora_provisioning().num_nodes(),
            "redshift_instance_type": self.redshift_provisioning().instance_type(),
            "redshift_num_nodes": self.redshift_provisioning().num_nodes(),
        }
        table_locations = self.table_locations_bitmap()
        return {**provisioning, **table_locations}
