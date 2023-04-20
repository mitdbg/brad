from typing import Callable, Dict, List, Set, Optional, Tuple

from brad.blueprint.provisioning import Provisioning
from brad.blueprint.table import Table
from brad.config.engine import Engine
from brad.routing import Router

RouterProvider = Callable[[], Router]


class Blueprint:
    def __init__(
        self,
        schema_name: str,
        table_schemas: List[Table],
        table_locations: Dict[str, List[Engine]],
        aurora_provisioning: Provisioning,
        redshift_provisioning: Provisioning,
        router_provider: Optional[RouterProvider],
    ):
        self._schema_name = schema_name
        self._table_schemas = table_schemas
        self._table_locations = table_locations
        self._aurora_provisioning = aurora_provisioning
        self._redshift_provisioning = redshift_provisioning
        self._router_provider = router_provider

        # Derived properties used for the class' methods.
        self._tables_by_name = {tbl.name: tbl for tbl in self._table_schemas}
        self._base_table_names = self._compute_base_tables()

    def schema_name(self) -> str:
        return self._schema_name

    def tables(self) -> List[Table]:
        return self._table_schemas

    def table_locations(self) -> Dict[str, List[Engine]]:
        return self._table_locations

    def tables_with_locations(self) -> List[Tuple[Table, List[Engine]]]:
        result = []
        for table_schema in self._table_schemas:
            result.append((table_schema, self.get_table_locations(table_schema.name)))
        return result

    def aurora_provisioning(self) -> Provisioning:
        return self._aurora_provisioning

    def redshift_provisioning(self) -> Provisioning:
        return self._redshift_provisioning

    def get_router(self) -> Optional[Router]:
        return self._router_provider() if self._router_provider is not None else None

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
