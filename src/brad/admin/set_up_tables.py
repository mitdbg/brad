import logging
from typing import List

from brad.config.dbtype import DBType
from brad.config.schema import Schema, Table, Column
from brad.config.strings import (
    delete_trigger_function_name,
    delete_trigger_name,
    seq_index_name,
    shadow_table_name,
    AURORA_EXTRACT_PROGRESS_TABLE_NAME,
    AURORA_SEQ_COLUMN,
)
from brad.config.extraction import ExtractionStrategy
from brad.config.file import ConfigFile
from brad.server.engine_connections import EngineConnections

logger = logging.getLogger(__name__)


# This method is called by `brad.exec.admin.main`.
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
    cxns = EngineConnections.connect_sync(config, autocommit=False)
    redshift = cxns.get_connection(DBType.Redshift).cursor()
    aurora = cxns.get_connection(DBType.Aurora).cursor()
    athena = cxns.get_connection(DBType.Athena).cursor()

    # 4. Set up the underlying tables.
    redshift_template = "CREATE TABLE {} ({}, PRIMARY KEY ({}));"
    athena_template = (
        "CREATE TABLE {} ({}) LOCATION '{}' TBLPROPERTIES ('table_type' = 'ICEBERG');"
    )

    for table in schema.tables:
        logger.info("Setting up table '%s'...", table.name)
        _set_up_aurora_table(aurora, table)

        primary_key_str = _pkey_str(table.primary_key)

        query = redshift_template.format(
            table.name, _col_str(table.columns, DBType.Redshift), primary_key_str
        )
        logger.debug("Running on Redshift: %s", query)
        redshift.execute(query)

        query = athena_template.format(
            table.name,
            _col_str(table.columns, DBType.Athena),
            "{}{}".format(config.athena_s3_data_path, table.name),
        )
        logger.debug("Running on Athena: %s", query)
        athena.execute(query)

    # 5. Create the extraction progress table.
    query = "CREATE TABLE {} (table_name TEXT PRIMARY KEY, next_extract_seq BIGINT, next_shadow_extract_seq BIGINT)".format(
        AURORA_EXTRACT_PROGRESS_TABLE_NAME,
    )
    logger.debug("Running on Aurora: %s", query)
    aurora.execute(query)

    # 6. Initialize extraction progress metadata for each table.
    initialize_template = "INSERT INTO {} (table_name, next_extract_seq, next_shadow_extract_seq) VALUES (?, 0, 0)".format(
        AURORA_EXTRACT_PROGRESS_TABLE_NAME
    )
    for table in schema.tables:
        logger.debug(
            "Running on Aurora: %s with value %s", initialize_template, table.name
        )
        aurora.execute(initialize_template, table.name)

    # 7. Commit the changes.
    aurora.commit()
    redshift.commit()
    # Athena does not support the notion of committing a transaction.

    # 8. Install the `aws_s3` extension (needed for data extraction).
    aurora.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE")

    logger.info("Done!")


def _set_up_aurora_table(cursor, table: Table):
    aurora_extract_main_template = (
        "CREATE TABLE {} ({}, " + AURORA_SEQ_COLUMN + " BIGSERIAL, PRIMARY KEY ({}));"
    )
    aurora_extract_shadow_template = (
        "CREATE TABLE {} ({}, " + AURORA_SEQ_COLUMN + " BIGSERIAL, PRIMARY KEY ({}));"
    )
    aurora_delete_trigger_fn_template = """
        CREATE OR REPLACE FUNCTION {trigger_fn_name}()
            RETURNS trigger AS
        $BODY$
        BEGIN
        INSERT INTO {shadow_table_name} ({pkey_cols}) VALUES ({pkey_vals});
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
    aurora_index_template = (
        "CREATE INDEX {} ON {} USING btree (" + AURORA_SEQ_COLUMN + ");"
    )

    primary_key_col_str = _col_str(table.primary_key, DBType.Aurora)
    primary_key_str = _pkey_str(table.primary_key)

    # Create the main table.
    query = aurora_extract_main_template.format(
        table.name,
        _col_str(table.columns, DBType.Aurora),
        primary_key_str,
    )
    logger.debug("Running on Aurora: %s", query)
    cursor.execute(query)

    # Create the shadow table (for deletes).
    query = aurora_extract_shadow_template.format(
        shadow_table_name(table), primary_key_col_str, primary_key_str
    )
    logger.debug("Running on Aurora: %s", query)
    cursor.execute(query)

    # Create the delete trigger function.
    query = aurora_delete_trigger_fn_template.format(
        trigger_fn_name=delete_trigger_function_name(table),
        shadow_table_name=shadow_table_name(table),
        pkey_cols=primary_key_str,
        pkey_vals=", ".join(
            map(lambda pkey: "OLD.{}".format(pkey.name), table.primary_key)
        ),
    )
    logger.debug("Running on Aurora: %s", query)
    cursor.execute(query)

    # Create the delete trigger.
    query = aurora_create_trigger_template.format(
        trigger_name=delete_trigger_name(table),
        table_name=table.name,
        trigger_fn_name=delete_trigger_function_name(table),
    )
    logger.debug("Running on Aurora: %s", query)
    cursor.execute(query)

    # Create the indexes.
    query = aurora_index_template.format(
        seq_index_name(table, for_shadow=False), table.name
    )
    logger.debug("Running on Aurora: %s", query)
    cursor.execute(query)

    query = aurora_index_template.format(
        seq_index_name(table, for_shadow=True), shadow_table_name(table)
    )
    logger.debug("Running on Aurora: %s", query)
    cursor.execute(query)


def _type_converter(data_type: str, for_db: DBType) -> str:
    # A hacky way to ensure we use a supported type in each DBMS (Athena does
    # not support `TEXT` data).
    if data_type.upper() == "TEXT" and for_db == DBType.Athena:
        return "STRING"
    else:
        return data_type


def _col_str(cols: List[Column], for_db: DBType) -> str:
    return ", ".join(
        map(
            lambda c: "{} {}".format(c.name, _type_converter(c.data_type, for_db)), cols
        )
    )


def _pkey_str(pkey_cols: List[Column]) -> str:
    return ", ".join(map(lambda c: c.name, pkey_cols))
