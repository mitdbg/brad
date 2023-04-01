from .table import Table, TableName

from typing import List, Set


class DataBlueprint:
    def __init__(
        self,
        schema_name: str,
        tables: List[Table],
    ):
        self._schema_name = schema_name
        self._tables = tables

        self._tables_by_name = {tbl.name: tbl for tbl in self._tables}
        self._base_table_names = self._compute_base_tables()

    @property
    def schema_name(self) -> str:
        return self._schema_name

    @property
    def tables(self) -> List[Table]:
        return self._tables

    @property
    def base_table_names(self) -> Set[TableName]:
        return self._base_table_names

    def get_table(self, table_name: str | TableName) -> Table:
        if isinstance(table_name, str):
            table_name = TableName(table_name)
        try:
            return self._tables_by_name[table_name]
        except KeyError as ex:
            raise ValueError from ex

    def _compute_base_tables(self) -> Set[TableName]:
        """
        Compute the base tables in the dependency graph. These are the tables
        with no dependencies.
        """
        visited: Set[TableName] = set()
        base_tables: Set[TableName] = set()

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

        for table in self._tables:
            visit_table(table)

        return base_tables
