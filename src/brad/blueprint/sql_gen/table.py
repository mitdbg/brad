from typing import List, Tuple

from brad.blueprint.data import DataBlueprint
from brad.blueprint.data.location import Location
from brad.blueprint.data.table import Column, Table
from brad.config.dbtype import DBType
from brad.config.file import ConfigFile
from brad.config.strings import (
    AURORA_EXTRACT_PROGRESS_TABLE_NAME,
    AURORA_SEQ_COLUMN,
    source_table_name,
    shadow_table_name,
    seq_index_name,
    delete_trigger_function_name,
    delete_trigger_name,
    update_trigger_function_name,
    update_trigger_name,
)
from ._table_templates import (
    AURORA_INDEX_TEMPLATE,
    AURORA_SEQ_CREATE_TABLE_TEMPLATE,
    AURORA_CREATE_SOURCE_VIEW_TEMPLATE,
    AURORA_DELETE_TRIGGER_FN_TEMPLATE,
    AURORA_TRIGGER_TEMPLATE,
    AURORA_BARE_OR_REDSHIFT_CREATE_TABLE_TEMPLATE,
    ATHENA_CREATE_TABLE_TEMPLATE,
    AURORA_UPDATE_TRIGGER_FN_TEMPLATE,
)


class TableSqlGenerator:
    def __init__(self, config: ConfigFile, blueprint: DataBlueprint):
        self._config = config
        self._blueprint = blueprint

    def generate_create_table_sql(
        self, table: Table, location: Location
    ) -> Tuple[List[str], DBType]:
        """
        Returns SQL queries that should be used to create `table` on `location`,
        along with the engine on which to execute the queries.
        """

        if location == Location.Aurora:
            if table.name in self._blueprint.base_table_names:
                # This table needs to support incremental extraction. We need to
                # create several additional structures to support this extraction.
                columns_with_types = comma_separated_column_names_and_types(
                    table.columns, DBType.Aurora
                )
                pkey_columns = comma_separated_column_names(table.primary_key)

                # The base source table, which includes a monotonically
                # increasing sequence column used for incremental extraction.
                create_source_table = AURORA_SEQ_CREATE_TABLE_TEMPLATE.format(
                    table_name=source_table_name(table),
                    columns=columns_with_types,
                    pkey_columns=pkey_columns,
                )
                # The index on the monotonically increasing column.
                create_source_index = AURORA_INDEX_TEMPLATE.format(
                    index_name=seq_index_name(table, for_shadow=False),
                    table_name=source_table_name(table),
                )
                # A view over the source table that excludes the monotonically
                # increasing sequence column.
                create_source_view = AURORA_CREATE_SOURCE_VIEW_TEMPLATE.format(
                    view_name=table.name.value,
                    source_table_name=source_table_name(table),
                    columns=comma_separated_column_names(table.columns),
                )

                # The shadow table, which holds the deleted rows' primary keys.
                create_shadow_table = AURORA_SEQ_CREATE_TABLE_TEMPLATE.format(
                    table_name=shadow_table_name(table),
                    columns=comma_separated_column_names_and_types(
                        table.primary_key, DBType.Aurora
                    ),
                    pkey_columns=pkey_columns,
                )
                # The index on the shadow table's monotonically increasing column.
                create_shadow_index = AURORA_INDEX_TEMPLATE.format(
                    index_name=seq_index_name(table, for_shadow=True),
                    table_name=shadow_table_name(table),
                )

                # The delete trigger function.
                create_trigger_fn = AURORA_DELETE_TRIGGER_FN_TEMPLATE.format(
                    trigger_fn_name=delete_trigger_function_name(table),
                    shadow_table_name=shadow_table_name(table),
                    pkey_cols=pkey_columns,
                    pkey_vals=", ".join(
                        map(lambda pkey: "OLD.{}".format(pkey.name), table.primary_key)
                    ),
                )
                # The delete trigger itself.
                create_trigger = AURORA_TRIGGER_TEMPLATE.format(
                    trigger_name=delete_trigger_name(table),
                    table_name=source_table_name(table),
                    trigger_fn_name=delete_trigger_function_name(table),
                    trigger_cond="AFTER DELETE",
                )

                # The update trigger function. Whenever a row is updated, we
                # also bump the monotonically increasing sequence column.
                create_update_trigger_fn = AURORA_UPDATE_TRIGGER_FN_TEMPLATE.format(
                    trigger_fn_name=update_trigger_function_name(table),
                    seq_col=AURORA_SEQ_COLUMN,
                    # This appears to be the PostgreSQL sequence naming format.
                    seq_name="{}_{}_seq".format(
                        source_table_name(table),
                        AURORA_SEQ_COLUMN,
                    ),
                )
                # The update trigger itself.
                create_update_trigger = AURORA_TRIGGER_TEMPLATE.format(
                    trigger_name=update_trigger_name(table),
                    table_name=source_table_name(table),
                    trigger_fn_name=update_trigger_function_name(table),
                    trigger_cond="BEFORE UPDATE",
                )

                return (
                    [
                        create_source_table,
                        create_source_index,
                        create_source_view,
                        create_shadow_table,
                        create_shadow_index,
                        create_trigger_fn,
                        create_trigger,
                        create_update_trigger_fn,
                        create_update_trigger,
                    ],
                    DBType.Aurora,
                )

            else:
                # This is just a regular table on Aurora that will not need to
                # support incremental extraction.
                sql = AURORA_BARE_OR_REDSHIFT_CREATE_TABLE_TEMPLATE.format(
                    table_name=table.name.value,
                    columns=comma_separated_column_names_and_types(
                        table.columns, DBType.Aurora
                    ),
                    pkey_columns=comma_separated_column_names(table.primary_key),
                )
                return ([sql], DBType.Aurora)

        elif location == Location.Redshift:
            sql = AURORA_BARE_OR_REDSHIFT_CREATE_TABLE_TEMPLATE.format(
                table_name=table.name.value,
                columns=comma_separated_column_names_and_types(
                    table.columns, DBType.Redshift
                ),
                pkey_columns=comma_separated_column_names(table.primary_key),
            )
            return ([sql], DBType.Redshift)

        elif location == Location.S3Iceberg:
            sql = ATHENA_CREATE_TABLE_TEMPLATE.format(
                table_name=table.name.value,
                columns=comma_separated_column_names_and_types(
                    table.columns, DBType.Athena
                ),
                s3_path="{}{}".format(
                    self._config.athena_s3_data_path, table.name.value
                ),
            )
            return ([sql], DBType.Athena)

        else:
            raise RuntimeError("Unsupported location {}".format(location))

    def generate_extraction_progress_set_up_table_sql(self) -> Tuple[List[str], DBType]:
        queries = []

        # Create the extraction progress table.
        create_extract_table = (
            "CREATE TABLE "
            + AURORA_EXTRACT_PROGRESS_TABLE_NAME
            + " (table_name TEXT PRIMARY KEY, next_extract_seq BIGINT, next_shadow_extract_seq BIGINT)"
        )
        queries.append(create_extract_table)

        # Initialize extraction progress metadata for each table.
        initialize_template = (
            "INSERT INTO "
            + AURORA_EXTRACT_PROGRESS_TABLE_NAME
            + " (table_name, next_extract_seq, next_shadow_extract_seq) VALUES ('{table_name}', 0, 0)"
        )
        for base_table_name in self._blueprint.base_table_names:
            base_table = self._blueprint.get_table(base_table_name)
            if Location.Aurora not in base_table.locations:
                continue
            queries.append(initialize_template.format(table_name=base_table_name))

        return (queries, DBType.Aurora)


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
