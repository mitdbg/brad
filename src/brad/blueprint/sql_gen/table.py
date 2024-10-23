from typing import List, Tuple

from brad.blueprint import Blueprint
from brad.blueprint.table import Column, Table
from brad.config.engine import Engine
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
    AURORA_CREATE_BTREE_INDEX_TEMPLATE,
    AURORA_DROP_INDEX_TEMPLATE,
    AURORA_SEQ_COL_INDEX_TEMPLATE,
    AURORA_SEQ_CREATE_TABLE_TEMPLATE,
    AURORA_CREATE_SOURCE_VIEW_TEMPLATE,
    AURORA_DELETE_TRIGGER_FN_TEMPLATE,
    AURORA_TRIGGER_TEMPLATE,
    AURORA_BARE_OR_REDSHIFT_CREATE_TABLE_TEMPLATE,
    ATHENA_CREATE_TABLE_TEMPLATE,
    AURORA_UPDATE_TRIGGER_FN_TEMPLATE,
    AURORA_2ND_INDEX_NAME_TEMPLATE,
)


class TableSqlGenerator:
    def __init__(self, config: ConfigFile, blueprint: Blueprint):
        self._config = config
        self._blueprint = blueprint

    def generate_create_table_sql(
        self, table: Table, location: Engine, bare_aurora_tables: bool = False
    ) -> Tuple[List[str], Engine]:
        """
        Returns SQL queries that should be used to create `table` on `location`,
        along with the engine on which to execute the queries.
        """

        if location == Engine.Aurora:
            if (
                not bare_aurora_tables
                and table.name in self._blueprint.base_table_names()
            ):
                # This table needs to support incremental extraction. We need to
                # create several additional structures to support this extraction.
                columns_with_types = comma_separated_column_names_and_types(
                    table.columns, Engine.Aurora
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
                create_source_index = AURORA_SEQ_COL_INDEX_TEMPLATE.format(
                    index_name=seq_index_name(table, for_shadow=False),
                    table_name=source_table_name(table),
                )
                # A view over the source table that excludes the monotonically
                # increasing sequence column.
                create_source_view = AURORA_CREATE_SOURCE_VIEW_TEMPLATE.format(
                    view_name=table.name,
                    source_table_name=source_table_name(table),
                    columns=comma_separated_column_names(table.columns),
                )

                # The shadow table, which holds the deleted rows' primary keys.
                create_shadow_table = AURORA_SEQ_CREATE_TABLE_TEMPLATE.format(
                    table_name=shadow_table_name(table),
                    columns=comma_separated_column_names_and_types(
                        table.primary_key, Engine.Aurora
                    ),
                    pkey_columns=pkey_columns,
                )
                # The index on the shadow table's monotonically increasing column.
                create_shadow_index = AURORA_SEQ_COL_INDEX_TEMPLATE.format(
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

                # Any secondary indexes.
                # NOTE: Aurora creates primary key indexes automatically.
                create_indexes = []
                for index_cols in table.secondary_indexed_columns:
                    col_names = list(map(lambda col: col.name, index_cols))
                    create_indexes.append(
                        AURORA_CREATE_BTREE_INDEX_TEMPLATE.format(
                            index_name=AURORA_2ND_INDEX_NAME_TEMPLATE.format(
                                table.name, "_".join(col_names)
                            ),
                            table_name=source_table_name(table),
                            columns=", ".join(col_names),
                        )
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
                        *create_indexes,
                    ],
                    Engine.Aurora,
                )

            else:
                # This is just a regular table on Aurora that will not need to
                # support incremental extraction.
                sql = AURORA_BARE_OR_REDSHIFT_CREATE_TABLE_TEMPLATE.format(
                    table_name=table.name,
                    columns=comma_separated_column_names_and_types(
                        table.columns, Engine.Aurora
                    ),
                    pkey_columns=comma_separated_column_names(table.primary_key),
                )
                return ([sql], Engine.Aurora)

        elif location == Engine.Redshift:
            sql = AURORA_BARE_OR_REDSHIFT_CREATE_TABLE_TEMPLATE.format(
                table_name=table.name,
                columns=comma_separated_column_names_and_types(
                    table.columns, Engine.Redshift
                ),
                pkey_columns=comma_separated_column_names(table.primary_key),
            )
            return ([sql], Engine.Redshift)

        elif location == Engine.Athena:
            sql = ATHENA_CREATE_TABLE_TEMPLATE.format(
                table_name=table.name,
                columns=comma_separated_column_names_and_types(
                    table.columns, Engine.Athena
                ),
                s3_path="{}{}/{}".format(
                    self._config.athena_s3_data_path,
                    self._blueprint.schema_name(),
                    table.name,
                ),
            )
            return ([sql], Engine.Athena)

        else:
            raise RuntimeError("Unsupported location {}".format(location))

    def generate_extraction_progress_set_up_table_sql(self) -> Tuple[List[str], Engine]:
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
        for base_table_name in self._blueprint.base_table_names():
            base_table_locations = self._blueprint.get_table_locations(base_table_name)
            if Engine.Aurora not in base_table_locations:
                continue
            queries.append(initialize_template.format(table_name=base_table_name))

        return (queries, Engine.Aurora)

    def generate_extraction_progress_init(
        self, table_name: str
    ) -> Tuple[List[str], Engine]:
        queries = []
        initialize_template = (
            "INSERT INTO "
            + AURORA_EXTRACT_PROGRESS_TABLE_NAME
            + " (table_name, next_extract_seq, next_shadow_extract_seq) VALUES ('{table_name}', 0, 0)"
        )
        base_table_names = self._blueprint.base_table_names()
        table_locations = self._blueprint.get_table_locations(table_name)
        if Engine.Aurora in table_locations and table_name in base_table_names:
            queries.append(initialize_template.format(table_name=table_name))
        return (queries, Engine.Aurora)

    def generate_rename_table_sql(
        self, table: Table, location: Engine, new_name: str
    ) -> Tuple[List[str], Engine]:
        """
        Generates the SQL statements needed to rename a table on the given engine.
        """
        if location == Engine.Aurora:
            # Aurora is more complicated because we use a view with other
            # metadata too. This is not currently needed.
            raise RuntimeError("Aurora renames are currently unimplemented.")

        elif location == Engine.Redshift or location == Engine.Athena:
            return ([f"ALTER TABLE {table.name} RENAME TO {new_name}"], location)

        else:
            raise RuntimeError(f"Unsupported location {str(location)}")


def generate_create_index_sql(
    table: Table, indexes: List[Tuple[Column, ...]]
) -> List[str]:
    create_indexes = []
    for index_cols in indexes:
        col_names = list(map(lambda col: col.name, index_cols))
        create_indexes.append(
            AURORA_CREATE_BTREE_INDEX_TEMPLATE.format(
                index_name=AURORA_2ND_INDEX_NAME_TEMPLATE.format(
                    table.name, "_".join(col_names)
                ),
                table_name=source_table_name(table),
                columns=", ".join(col_names),
            )
        )
    return create_indexes


def generate_drop_index_sql(
    table: Table, indexes: List[Tuple[Column, ...]]
) -> List[str]:
    drop_indexes = []
    for index_cols in indexes:
        col_names = list(map(lambda col: col.name, index_cols))
        drop_indexes.append(
            AURORA_DROP_INDEX_TEMPLATE.format(
                index_name=AURORA_2ND_INDEX_NAME_TEMPLATE.format(
                    table.name, "_".join(col_names)
                ),
            )
        )
    return drop_indexes


def comma_separated_column_names(cols: List[Column]) -> str:
    return ", ".join(map(lambda c: c.name, cols))


def comma_separated_column_names_and_types(cols: List[Column], for_db: Engine) -> str:
    return ", ".join(
        map(
            lambda c: "{} {}".format(c.name, _type_for(c.data_type, for_db)),
            cols,
        )
    )


def _type_for(data_type: str, for_db: Engine) -> str:
    # A hacky way to ensure we use a supported type in each DBMS (e.g. Athena does
    # not support `TEXT` data).
    data_type_upper = data_type.upper()
    if data_type_upper.startswith("CHARACTER") or data_type_upper.startswith("CHAR("):
        if for_db == Engine.Athena:
            return "STRING"
        elif for_db == Engine.Redshift and data_type_upper == "CHARACTER VARYING":
            return "VARCHAR(MAX)"
        elif for_db == Engine.Redshift and data_type_upper.startswith(
            "CHARACTER VARYING"
        ):
            return "VARCHAR(256)"
    if data_type_upper == "TEXT" and for_db == Engine.Athena:
        return "STRING"
    elif data_type_upper == "SERIAL" and (
        for_db == Engine.Athena or for_db == Engine.Redshift
    ):
        return "BIGINT"
    elif data_type_upper.startswith("VARCHAR") and for_db == Engine.Athena:
        return "STRING"
    elif data_type_upper.startswith("VECTOR"):
        if for_db == Engine.Athena:
            return "BINARY"
        elif for_db == Engine.Redshift:
            return "VARBYTE"
        else:
            return data_type
    else:
        return data_type
