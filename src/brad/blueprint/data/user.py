import yaml

from .table import Column, UserProvidedTable
from typing import List, Set


class UserProvidedDataBlueprint:
    """
    Represents a "user-provided" logical data blueprint. This data blueprint
    contains a list of the tables, their schemas, dependencies, and transforms
    between the tables.

    BRAD's planner will convert this user-provided logical blueprint into a
    physical data blueprint (contains details about table placement and
    replication).
    """

    @classmethod
    def load_from_yaml_file(cls, path: str) -> "UserProvidedDataBlueprint":
        with open(path, "r", encoding="UTF-8") as file:
            raw_yaml = yaml.load(file, Loader=yaml.Loader)
        return cls._load_from_raw_yaml(raw_yaml)

    @classmethod
    def load_from_yaml_str(cls, yaml_str: str) -> "UserProvidedDataBlueprint":
        return cls._load_from_raw_yaml(yaml.load(yaml_str, Loader=yaml.Loader))

    @classmethod
    def _load_from_raw_yaml(cls, raw_yaml) -> "UserProvidedDataBlueprint":
        tables: List[UserProvidedTable] = []
        for raw_table in raw_yaml["tables"]:
            table_name = raw_table["table_name"]
            columns = list(
                map(UserProvidedDataBlueprint._parse_column, raw_table["columns"])
            )
            table_deps = (
                raw_table["dependencies"] if "dependencies" in raw_table else []
            )
            transform = raw_table["transform"] if "transform" in raw_table else None
            tables.append(UserProvidedTable(table_name, columns, table_deps, transform))

        return cls(raw_yaml["database_name"], tables)

    def __init__(self, db_name: str, tables: List[UserProvidedTable]):
        self._db_name = db_name
        self._tables = tables

    @property
    def db_name(self) -> str:
        return self._db_name

    @property
    def tables(self) -> List[UserProvidedTable]:
        return self._tables

    def validate(self) -> None:
        """
        Checks the user-declared tables and ensures that there are (i) no
        dependency cycles, and (ii) no dependencies on undeclared tables.
        """

        tables_by_name = {tbl.name: tbl for tbl in self.tables}
        checked: Set[str] = set()
        curr_path: Set[str] = set()

        # Recursively checks for circular dependencies.
        def check_deps(table: UserProvidedTable):
            if table.name in checked:
                return
            if table.name in curr_path:
                raise RuntimeError(
                    "Detected dependency cycle involving '{}'.".format(table.name)
                )
            curr_path.add(table.name)

            for dep in table.table_dependencies:
                if dep not in tables_by_name:
                    raise RuntimeError(
                        "Table '{}' depends on undeclared table '{}'".format(
                            table.name, dep
                        )
                    )
                check_deps(tables_by_name[dep])

            curr_path.remove(table.name)
            checked.add(table.name)

        for tbl in self.tables:
            check_deps(tbl)

    @staticmethod
    def _parse_column(raw_column) -> Column:
        return Column(
            raw_column["name"],
            raw_column["data_type"],
            "primary_key" in raw_column and raw_column["primary_key"],
        )
