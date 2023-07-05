import logging

from brad.asset_manager import AssetManager
from brad.blueprint.user import UserProvidedBlueprint
from brad.blueprint.sql_gen.table import TableSqlGenerator
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.planner.data import bootstrap_blueprint
from brad.server.blueprint_manager import BlueprintManager
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

    # 2. Load and validate the user-provided schema.
    user = UserProvidedBlueprint.load_from_yaml_file(args.schema_file)
    user.validate()

    # 3. Get the bootstrapped blueprint. Later on this planning phase will be
    # more sophisticated (we'll take the "workload" as input).
    blueprint = bootstrap_blueprint(user)

    # 4. Connect to the underlying engines and create "databases" for the
    # schema.
    cxns = EngineConnections.connect_sync(config, autocommit=True)
    create_schema = "CREATE DATABASE {}".format(blueprint.schema_name())
    cxns.get_connection(Engine.Aurora).cursor_sync().execute_sync(create_schema)
    cxns.get_connection(Engine.Athena).cursor_sync().execute_sync(create_schema)
    cxns.get_connection(Engine.Redshift).cursor_sync().execute_sync(create_schema)
    cxns.close_sync()
    del cxns

    # 5. Now re-connect to the underlying engines with the schema name.
    cxns = EngineConnections.connect_sync(
        config, schema_name=blueprint.schema_name(), autocommit=False
    )
    redshift = cxns.get_connection(Engine.Redshift).cursor_sync()
    aurora = cxns.get_connection(Engine.Aurora).cursor_sync()

    # 6. Set up the underlying tables.
    sql_gen = TableSqlGenerator(config, blueprint)

    for table in blueprint.tables():
        table_locations = blueprint.get_table_locations(table.name)
        for location in table_locations:
            logger.info(
                "Creating table '%s' on %s...",
                table.name,
                location,
            )
            queries, db_type = sql_gen.generate_create_table_sql(table, location)
            conn = cxns.get_connection(db_type)
            cursor = conn.cursor_sync()
            for q in queries:
                logger.debug("Running on %s: %s", str(db_type), q)
                cursor.execute_sync(q)

    # 7. Create and set up the extraction progress table.
    queries, db_type = sql_gen.generate_extraction_progress_set_up_table_sql()
    conn = cxns.get_connection(db_type)
    cursor = conn.cursor_sync()
    for q in queries:
        logger.debug("Running on %s: %s", str(db_type), q)
        cursor.execute_sync(q)

    # 9. Commit the changes.
    aurora.commit_sync()
    redshift.commit_sync()
    # Athena does not support the notion of committing a transaction.

    # 10. Install the `aws_s3` extension (needed for data extraction).
    aurora.execute_sync("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE")
    aurora.commit_sync()

    # 11. Persist the data blueprint.
    assets = AssetManager(config)
    blueprint_mgr = BlueprintManager(assets, blueprint.schema_name())
    blueprint_mgr.set_blueprint(blueprint)
    blueprint_mgr.persist_sync()

    logger.info("Done!")
