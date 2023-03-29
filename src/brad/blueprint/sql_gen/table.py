from brad.blueprint.data import DataBlueprint
from brad.blueprint.data.location import Location
from brad.blueprint.data.table import Column, TableLocation
from brad.config.dbtype import DBType
from brad.config.file import ConfigFile

from typing import List, Tuple

_AURORA_REDSHIFT_CREATE_TABLE_TEMPLATE = (
    "CREATE TABLE {table_name} ({columns}, PRIMARY KEY ({pkey_columns}));"
)

_ATHENA_CREATE_TABLE_TEMPLATE = "CREATE TABLE {table_name} ({columns}) LOCATION '{s3_path}' TBLPROPERTIES ('table_type' = 'ICEBERG');"


class TableSqlGenerator:
    def __init__(self, config: ConfigFile, blueprint: DataBlueprint):
        self._config = config
        self._blueprint = blueprint

    def generate_create_table_sql(
        self, table_location: TableLocation
    ) -> Tuple[str, DBType]:
        """
        Returns a SQL query that should be used to create the table given by
        `table_location`, along with the engine on which to execute the query.
        """

        table = self._blueprint.table_schema_for(table_location.table_name)
        location = table_location.location

        if location == Location.Aurora or location == Location.Redshift:
            sql = _AURORA_REDSHIFT_CREATE_TABLE_TEMPLATE.format(
                table_name=table.name,
                columns=comma_separated_column_names_and_types(
                    table.columns, DBType.Redshift
                ),
                pkey_columns=comma_separated_column_names(table.primary_key),
            )
            return (
                sql,
                DBType.Aurora if location == Location.Aurora else DBType.Redshift,
            )

        elif location == Location.S3Iceberg:
            sql = _ATHENA_CREATE_TABLE_TEMPLATE.format(
                table_name=table.name,
                columns=comma_separated_column_names_and_types(
                    table.columns, DBType.Athena
                ),
                s3_path="{}{}".format(self._config.athena_s3_data_path, table.name),
            )
            return (sql, DBType.Athena)

        else:
            raise RuntimeError("Unsupported location {}".format(location))


def comma_separated_column_names(cols: List[Column]) -> str:
    return ", ".join(map(lambda c: c.name, cols))


def comma_separated_column_names_and_types(cols: List[Column], for_db: DBType) -> str:
    return ", ".join(
        map(
            lambda c: "{} {}".format(c.name, _type_for(c.data_type, for_db)),
            cols,
        )
    )


def _type_for(data_type: str, for_db: DBType) -> str:
    # A hacky way to ensure we use a supported type in each DBMS (Athena does
    # not support `TEXT` data).
    if data_type.upper() == "TEXT" and for_db == DBType.Athena:
        return "STRING"
    else:
        return data_type
