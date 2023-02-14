from typing import List

from iohtap.config.dbtype import DBType
from iohtap.config.schema import Schema, Table, Column
from iohtap.config.strings import (
    delete_trigger_function_name,
    delete_trigger_name,
    seq_index_name,
    shadow_table_name,
    aurora_extract_progress_table_name,
)
from iohtap.config.extraction import ExtractionStrategy
from iohtap.config.file import ConfigFile
from iohtap.server.db_connection_manager import DBConnectionManager


# This method is called by `iohtap.exec.admin.main`.
def set_up_tables(args):
    # 1. Load the schema file.
    schema = Schema.load(args.schema_file)

    # 2. Load the config.
    config = ConfigFile(args.config_file)
    if config.extraction_strategy != ExtractionStrategy.SequenceTrigger:
        raise NotImplementedError(
            "Unsupported extraction strategy: {}".format(
                str(config.extraction_strategy)
            )
        )

    # 3. Connect to the underlying engines.
    cxns = DBConnectionManager(config)
    redshift = cxns.get_connection(DBType.Redshift).cursor()
    aurora = cxns.get_connection(DBType.Aurora).cursor()
    athena = cxns.get_connection(DBType.Athena).cursor()

    # 4. Set up the underlying tables.
    redshift_template = "CREATE TABLE {} ({}, PRIMARY KEY ({}));"
    athena_template = "CREATE TABLE {} ({}, PRIMARY KEY ({})) LOCATION '{}' TBLPROPERTIES ('table_type' = 'ICEBERG');"

    for table in schema.tables:
        _set_up_aurora_table(aurora, table)

        col_str = _col_str(table.columns)
        primary_key_str = _pkey_str(table.primary_key)
        redshift.execute(redshift_template.format(table.name, col_str, primary_key_str))
        athena.execute(
            athena_template.format(
                table.name, col_str, primary_key_str, config.athena_s3_data_path
            )
        )

    # 5. Create the extraction progress table.
    aurora.execute(
        "CREATE TABLE {} (table_name TEXT PRIMARY KEY, next_extract_seq BIGINT, next_shadow_extract_seq BIGINT)".format(
            aurora_extract_progress_table_name(),
        )
    )

    # 6. Commit the changes.
    aurora.commit()
    redshift.commit()
    # Athena does not support the notion of committing a transaction.


def _set_up_aurora_table(cursor, table: Table):
    aurora_extract_main_template = (
        "CREATE TABLE {} ({}, iohtap_seq BIGSERIAL, PRIMARY KEY ({}));"
    )
    aurora_extract_shadow_template = (
        "CREATE TABLE {} ({}, iohtap_seq BIGSERIAL, PRIMARY KEY ({}));"
    )
    aurora_delete_trigger_fn_template = """
        CREATE OR REPLACE FUNCTION {trigger_fn_name}()
            RETURNS trigger AS
        $BODY$
        BEGIN
        INSERT INTO {shadow_table_name} ({pkey_cols}) VALUES ({pkey_vals})
        RETURN NULL;
        END;
        $BODY$
        LANGUAGE plpgsql VOLATILE;
    """
    aurora_create_trigger_template = """
        CREATE TRIGGER {trigger_name}
        AFTER DELETE ON {table_name}
        FOR EACH ROW
        EXECUTE PROCEDURE {trigger_fn_name}();
    """
    aurora_index_template = "CREATE INDEX {} ON {} USING btree (iohtap_seq);"

    primary_key_str = _pkey_str(table.primary_key)

    # Create the main table.
    cursor.execute(
        aurora_extract_main_template.format(
            table.name,
            _col_str(table.columns),
            primary_key_str,
        )
    )

    # Create the shadow table (for deletes).
    cursor.execute(
        aurora_extract_shadow_template.format(
            shadow_table_name(table), primary_key_str, primary_key_str
        )
    )

    # Create the delete trigger function.
    cursor.execute(
        aurora_delete_trigger_fn_template.format(
            trigger_fn_name=delete_trigger_function_name(table),
            shadow_table_name=shadow_table_name(table),
            pkey_cols=primary_key_str,
            pkey_vals=", ".join(
                map(lambda pkey: "OLD.{}".format(pkey.name), table.primary_key)
            ),
        )
    )

    # Create the delete trigger.
    cursor.execute(
        aurora_create_trigger_template.format(
            trigger_name=delete_trigger_name(table),
            table_name=table.name,
            trigger_fn_name=delete_trigger_function_name(table),
        )
    )

    # Create the indexes.
    cursor.execute(
        aurora_index_template.format(
            seq_index_name(table, for_shadow=False), table.name
        )
    )
    cursor.execute(
        aurora_index_template.format(
            seq_index_name(table, for_shadow=True), shadow_table_name(table)
        )
    )


def _col_str(cols: List[Column]) -> str:
    return ", ".join(map(lambda c: "{} {}".format(c.name, c.data_type), cols))


def _pkey_str(pkey_cols: List[Column]) -> str:
    return ", ".join(map(lambda c: c.name, pkey_cols))
