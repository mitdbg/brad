import yaml

from .table import Column, UserProvidedTable
from typing import List


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
    def load_from_yaml_file(cls, path: str):
        with open(path, "r", encoding="UTF-8") as file:
            raw_yaml = yaml.load(file, Loader=yaml.Loader)

        tables: List[UserProvidedTable] = []
        for raw_table in raw_yaml["tables"]:
            table_name = raw_table["table_name"]
            columns = list(
                map(UserProvidedDataBlueprint._parse_column, raw_table["columns"])
            )
            table_deps = raw_table["dependencies"]
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

    @staticmethod
    def _parse_column(raw_column) -> Column:
        return Column(
            raw_column["name"],
            raw_column["data_type"],
            "primary_key" in raw_column and raw_column["primary_key"],
        )
