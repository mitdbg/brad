import asyncio
import logging

from brad.asset_manager import AssetManager
from brad.blueprint.user import UserProvidedBlueprint
from brad.blueprint.sql_gen.table import TableSqlGenerator
from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.front_end.engine_connections import EngineConnections
from brad.planner.data import bootstrap_blueprint
from brad.provisioning.directory import Directory

logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "bootstrap_schema", help="Set up a new schema on BRAD."
    )
    parser.add_argument(
        "--physical-config-file",
        type=str,
        required=True,
        help="Path to BRAD's physical configuration file.",
    )
    parser.add_argument(
        "--schema-file",
        type=str,
        help="Path to the database schema to bootstrap.",
    )
    parser.add_argument(
        "--only-engines",
        type=str,
        nargs="+",
        help="Use this flag to bootstrap only a subset of the engines. "
        "Only meant to be used if you know what you are doing!",
    )
    parser.add_argument(
        "--skip-persisting-blueprint",
        action="store_true",
        help="Set this flag to avoid persisting the blueprint. "
        "Only meant to be used if you know what you are doing!",
    )
    parser.add_argument(
        "--bare-aurora-tables",
        action="store_true",
        help="If set, do not create the shadow tables and triggers "
        "for Aurora. Only meant to be used if you know what you are doing!",
    )
    parser.set_defaults(admin_action=bootstrap_schema)


# This method is called by `brad.exec.admin.main`.
def bootstrap_schema(args):
    # 1. Load the config.
    config = ConfigFile.load_from_physical_config(args.physical_config_file)

    # 2. Load and validate the user-provided schema.
    user = UserProvidedBlueprint.load_from_yaml_file(args.schema_file)
    user.validate()

    # 3. Get the bootstrapped blueprint. Later on this planning phase will be
    # more sophisticated (we'll take the "workload" as input).
    blueprint = bootstrap_blueprint(user)

    # 4. Connect to the underlying engines and create "databases" for the
    # schema.
    directory = Directory(config)
    asyncio.run(directory.refresh())

    # Check for specific engines.
    if args.only_engines is not None and len(args.only_engines) > 0:
        engines_filter = {Engine.from_str(val) for val in args.only_engines}
    else:
        engines_filter = {Engine.Aurora, Engine.Athena, Engine.Redshift}

    cxns = EngineConnections.connect_sync(
        config, directory, autocommit=True, specific_engines=engines_filter
    )
    create_schema = "CREATE DATABASE {}".format(blueprint.schema_name())
    for engine in engines_filter:
        cxns.get_connection(engine).cursor_sync().execute_sync(create_schema)
    cxns.close_sync()
    del cxns

    # 5. Now re-connect to the underlying engines with the schema name.
    cxns = EngineConnections.connect_sync(
        config,
        directory,
        schema_name=blueprint.schema_name(),
        autocommit=False,
        specific_engines=engines_filter,
    )

    # 6. Set up the underlying tables.
    sql_gen = TableSqlGenerator(config, blueprint)

    for table in blueprint.tables():
        table_locations = blueprint.get_table_locations(table.name)
        for location in table_locations:
            if location not in engines_filter:
                continue
            logger.info(
                "Creating table '%s' on %s...",
                table.name,
                location,
            )
            queries, db_type = sql_gen.generate_create_table_sql(
                table, location, args.bare_aurora_tables
            )
            conn = cxns.get_connection(db_type)
            cursor = conn.cursor_sync()
            for q in queries:
                logger.debug("Running on %s: %s", str(db_type), q)
                cursor.execute_sync(q)

    # 7. Create and set up the extraction progress table.
    if Engine.Aurora in engines_filter and not args.bare_aurora_tables:
        queries, db_type = sql_gen.generate_extraction_progress_set_up_table_sql()
        conn = cxns.get_connection(db_type)
        cursor = conn.cursor_sync()
        for q in queries:
            logger.debug("Running on %s: %s", str(db_type), q)
            cursor.execute_sync(q)

    # 9. Commit the changes.
    # N.B. Athena does not support the notion of committing a transaction.
    if Engine.Aurora in engines_filter:
        cxns.get_connection(Engine.Aurora).cursor_sync().commit_sync()
    if Engine.Redshift in engines_filter:
        cxns.get_connection(Engine.Redshift).cursor_sync().commit_sync()

    # 10. Install the `aws_s3` extension (needed for data extraction).
    if Engine.Aurora in engines_filter:
        aurora = cxns.get_connection(Engine.Aurora)
        cursor = aurora.cursor_sync()
        cursor.execute_sync("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE")
        cursor.commit_sync()

    # 11. Persist the data blueprint.
    if not args.skip_persisting_blueprint:
        assets = AssetManager(config)
        BlueprintManager.initialize_schema(assets, blueprint)

    logger.info("Done!")
