import yaml
import pathlib
from typing import List, Set, Dict

from brad.config.engine import Engine
from .provisioning import Provisioning
from .table import Column, Table


class UserProvidedBlueprint:
    """
    Represents a "user-provided" logical blueprint. This logical blueprint
    contains

      - A list of the tables
      - The table schemas
      - Table dependencies
      - Transforms across tables
      - A starting provisioning for Redshift and Aurora

    BRAD's planner will convert this user-provided logical blueprint into a
    physical blueprint (contains details about table placement and replication).
    """

    @classmethod
    def load_from_yaml_file(cls, path: str | pathlib.Path) -> "UserProvidedBlueprint":
        with open(path, "r", encoding="UTF-8") as file:
            raw_yaml = yaml.load(file, Loader=yaml.Loader)
        return cls._load_from_raw_yaml(raw_yaml)

    @classmethod
    def load_from_yaml_str(cls, yaml_str: str) -> "UserProvidedBlueprint":
        return cls._load_from_raw_yaml(yaml.load(yaml_str, Loader=yaml.Loader))

    @classmethod
    def _load_from_raw_yaml(cls, raw_yaml) -> "UserProvidedBlueprint":
        tables: List[Table] = []
        bootstrap_locations: Dict[str, Set[Engine]] = {}
        for raw_table in raw_yaml["tables"]:
            name = raw_table["table_name"]
            columns = list(
                map(UserProvidedBlueprint._parse_column, raw_table["columns"])
            )
            table_deps: List[str] = (
                list(raw_table["dependencies"]) if "dependencies" in raw_table else []
            )
            transform = raw_table["transform"] if "transform" in raw_table else None

            secondary_indexed_columns = []
            if "indexes" in raw_table:
                column_map = {c.name: c for c in columns}
                for indexed_cols in raw_table["indexes"]:
                    col_list = []
                    for col_name in indexed_cols.split(","):
                        clean_col = col_name.strip()
                        try:
                            col_list.append(column_map[clean_col])
                        except KeyError as ex:
                            raise RuntimeError(
                                "Invalid index column: '{}' is not a column of '{}'".format(
                                    clean_col, name
                                )
                            ) from ex
                    secondary_indexed_columns.append(tuple(col_list))

            tables.append(
                Table(name, columns, table_deps, transform, secondary_indexed_columns)
            )

            if "bootstrap_locations" in raw_table:
                engine_set = {
                    Engine.from_str(engine)
                    for engine in raw_table["bootstrap_locations"]
                }
                bootstrap_locations[name] = engine_set

        if "provisioning" in raw_yaml:
            aurora = raw_yaml["provisioning"]["aurora"]
            redshift = raw_yaml["provisioning"]["redshift"]
            aurora_provisioning = Provisioning(
                aurora["instance_type"], aurora["num_nodes"]
            )
            redshift_provisioning = Provisioning(
                redshift["instance_type"], redshift["num_nodes"]
            )
        else:
            # These are our defaults.
            aurora_provisioning = Provisioning(
                instance_type="db.r6g.large", num_nodes=1
            )
            redshift_provisioning = Provisioning(instance_type="dc2.large", num_nodes=1)

        return cls(
            raw_yaml["schema_name"],
            tables,
            aurora_provisioning,
            redshift_provisioning,
            bootstrap_locations,
        )

    def __init__(
        self,
        schema_name: str,
        tables: List[Table],
        aurora_provisioning: Provisioning,
        redshift_provisioning: Provisioning,
        bootstrap_locations: Dict[str, Set[Engine]],
    ):
        self._schema_name = schema_name
        self._tables = tables
        self._aurora_provisioning = aurora_provisioning
        self._redshift_provisioning = redshift_provisioning
        self._bootstrap_locations = bootstrap_locations

    @property
    def schema_name(self) -> str:
        return self._schema_name

    @property
    def tables(self) -> List[Table]:
        return self._tables

    def aurora_provisioning(self) -> Provisioning:
        return self._aurora_provisioning

    def redshift_provisioning(self) -> Provisioning:
        return self._redshift_provisioning

    def bootstrap_locations(self) -> Dict[str, Set[Engine]]:
        return self._bootstrap_locations

    def validate(self) -> None:
        """
        Checks the user-declared tables and ensures that there are (i) no
        dependency cycles, and (ii) no dependencies on undeclared tables.
        """

        tables_by_name = {tbl.name: tbl for tbl in self.tables}
        checked: Set[str] = set()
        curr_path: Set[str] = set()

        # Recursively checks for circular dependencies.
        def check_deps(table: Table):
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
