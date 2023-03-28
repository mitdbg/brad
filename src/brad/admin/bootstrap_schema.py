import logging

from brad.blueprint.data.table import TableSchema
from brad.blueprint.data.user import UserProvidedDataBlueprint
from brad.blueprint.sql_gen.table import (
    comma_separated_column_names_and_types,
    comma_separated_column_names,
)
from brad.config.dbtype import DBType
from brad.config.strings import (
    delete_trigger_function_name,
    delete_trigger_name,
    seq_index_name,
    shadow_table_name,
    AURORA_EXTRACT_PROGRESS_TABLE_NAME,
    AURORA_SEQ_COLUMN,
)
from brad.config.file import ConfigFile
from brad.planner.data import bootstrap_data_blueprint
from brad.server.data_blueprint_manager import DataBlueprintManager
from brad.server.engine_connections import EngineConnections

logger = logging.getLogger(__name__)


# This method is called by `brad.exec.admin.main`.
def bootstrap_schema(args):
    # 1. Load the config.
    config = ConfigFile(args.config_file)

    # 2. Load the user-provided data schema.
    user = UserProvidedDataBlueprint.load_from_yaml_file(args.bootstrap_schema_file)

    # 3. Get the bootstrapped blueprint. Later on this planning phase will be
    # more sophisticated (we'll take the "workload" as input).
    blueprint = bootstrap_data_blueprint(user)

    # 4. Connect to the underlying engines and create "databases" for the
    # schema.
    cxns = EngineConnections.connect_sync(config, autocommit=True)
    create_schema = "CREATE DATABASE {}".format(blueprint.schema_name)
    cxns.get_connection(DBType.Aurora).cursor().execute(create_schema)
    cxns.get_connection(DBType.Athena).cursor().execute(create_schema)
    cxns.get_connection(DBType.Redshift).cursor().execute(create_schema)
    cxns.close_sync()
    del cxns

    # 5. Now re-connect to the underlying engines with the schema name.
    cxns = EngineConnections.connect_sync(
        config, schema_name=blueprint.schema_name, autocommit=False
    )
    redshift = cxns.get_connection(DBType.Redshift).cursor()
    aurora = cxns.get_connection(DBType.Aurora).cursor()
    athena = cxns.get_connection(DBType.Athena).cursor()

    # 6. Set up the underlying tables.
    redshift_template = "CREATE TABLE {} ({}, PRIMARY KEY ({}));"
    athena_template = (
        "CREATE TABLE {} ({}) LOCATION '{}' TBLPROPERTIES ('table_type' = 'ICEBERG');"
    )

    for table_name in blueprint.table_names():
        logger.info("Setting up table '%s'...", table_name)
        table = blueprint.table_schema_for(table_name)
        _set_up_aurora_table(aurora, table)

        primary_key_str = comma_separated_column_names(table.primary_key)

        query = redshift_template.format(
            table.name,
            comma_separated_column_names_and_types(table.columns, DBType.Redshift),
            primary_key_str,
        )
        logger.debug("Running on Redshift: %s", query)
        redshift.execute(query)

        query = athena_template.format(
            table.name,
            comma_separated_column_names_and_types(table.columns, DBType.Athena),
            "{}{}".format(config.athena_s3_data_path, table.name),
        )
        logger.debug("Running on Athena: %s", query)
        athena.execute(query)

    # 7. Create the extraction progress table.
    query = "CREATE TABLE {} (table_name TEXT PRIMARY KEY, next_extract_seq BIGINT, next_shadow_extract_seq BIGINT)".format(
        AURORA_EXTRACT_PROGRESS_TABLE_NAME,
    )
    logger.debug("Running on Aurora: %s", query)
    aurora.execute(query)

    # 8. Initialize extraction progress metadata for each table.
    initialize_template = "INSERT INTO {} (table_name, next_extract_seq, next_shadow_extract_seq) VALUES (?, 0, 0)".format(
        AURORA_EXTRACT_PROGRESS_TABLE_NAME
    )
    for table_name in blueprint.table_names():
        table = blueprint.table_schema_for(table_name)
        logger.debug(
            "Running on Aurora: %s with value %s", initialize_template, table.name
        )
        aurora.execute(initialize_template, table.name)

    # 9. Commit the changes.
    aurora.commit()
    redshift.commit()
    # Athena does not support the notion of committing a transaction.

    # 10. Install the `aws_s3` extension (needed for data extraction).
    aurora.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE")

    # 11. Persist the data blueprint.
    data_blueprint_mgr = DataBlueprintManager(config, blueprint.schema_name)
    data_blueprint_mgr.set_blueprint(blueprint)
    data_blueprint_mgr.persist_sync()

    logger.info("Done!")


def _set_up_aurora_table(cursor, table: TableSchema):
    # NOTE: Table extraction will be overhauled.
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

    primary_key_col_str = comma_separated_column_names_and_types(
        table.primary_key, DBType.Aurora
    )
    primary_key_str = comma_separated_column_names(table.primary_key)

    # Create the main table.
    query = aurora_extract_main_template.format(
        table.name,
        comma_separated_column_names_and_types(table.columns, DBType.Aurora),
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
