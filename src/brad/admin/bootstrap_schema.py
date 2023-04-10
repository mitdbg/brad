import logging

from brad.blueprint.data.user import UserProvidedDataBlueprint
from brad.blueprint.sql_gen.table import TableSqlGenerator
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.planner.data import bootstrap_data_blueprint
from brad.server.data_blueprint_manager import DataBlueprintManager
from brad.server.engine_connections import EngineConnections

logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "bootstrap_schema", help="Set up a new schema on BRAD."
    )
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to BRAD's configuration file.",
    )
    parser.add_argument(
        "--schema-file",
        type=str,
        help="Path to the database schema to bootstrap.",
    )
    parser.set_defaults(admin_action=bootstrap_schema)


# This method is called by `brad.exec.admin.main`.
def bootstrap_schema(args):
    # 1. Load the config.
    config = ConfigFile(args.config_file)

    # 2. Load the user-provided data schema.
    user = UserProvidedDataBlueprint.load_from_yaml_file(args.schema_file)

    # 3. Get the bootstrapped blueprint. Later on this planning phase will be
    # more sophisticated (we'll take the "workload" as input).
    blueprint = bootstrap_data_blueprint(user)

    # 4. Connect to the underlying engines and create "databases" for the
    # schema.
    cxns = EngineConnections.connect_sync(config, autocommit=True)
    create_schema = "CREATE DATABASE {}".format(blueprint.schema_name)
    cxns.get_connection(Engine.Aurora).cursor().execute(create_schema)
    cxns.get_connection(Engine.Athena).cursor().execute(create_schema)
    cxns.get_connection(Engine.Redshift).cursor().execute(create_schema)
    cxns.close_sync()
    del cxns

    # 5. Now re-connect to the underlying engines with the schema name.
    cxns = EngineConnections.connect_sync(
        config, schema_name=blueprint.schema_name, autocommit=False
    )
    redshift = cxns.get_connection(Engine.Redshift).cursor()
    aurora = cxns.get_connection(Engine.Aurora).cursor()

    # 6. Set up the underlying tables.
    sql_gen = TableSqlGenerator(config, blueprint)

    for table in blueprint.tables:
        for location in table.locations:
            logger.info(
                "Creating table '%s' on %s...",
                table.name,
                location,
            )
            queries, db_type = sql_gen.generate_create_table_sql(table, location)
            conn = cxns.get_connection(db_type)
            cursor = conn.cursor()
            for q in queries:
                logger.debug("Running on %s: %s", str(db_type), q)
                cursor.execute(q)

    # 7. Create and set up the extraction progress table.
    queries, db_type = sql_gen.generate_extraction_progress_set_up_table_sql()
    conn = cxns.get_connection(db_type)
    cursor = conn.cursor()
    for q in queries:
        logger.debug("Running on %s: %s", str(db_type), q)
        cursor.execute(q)

    # 9. Commit the changes.
    aurora.commit()
    redshift.commit()
    # Athena does not support the notion of committing a transaction.

    # 10. Install the `aws_s3` extension (needed for data extraction).
    aurora.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE")
    aurora.commit()

    # 11. Persist the data blueprint.
    data_blueprint_mgr = DataBlueprintManager(config, blueprint.schema_name)
    data_blueprint_mgr.set_blueprint(blueprint)
    data_blueprint_mgr.persist_sync()

    logger.info("Done!")
