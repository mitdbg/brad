import asyncio
import logging

from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.blueprint.user import UserProvidedBlueprint
from brad.blueprint.sql_gen.table import TableSqlGenerator
from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.front_end.engine_connections import EngineConnections
from brad.planner.data import bootstrap_blueprint

logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "alter_schema", help="Alters an existing schema on BRAD."
    )
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to BRAD's configuration file.",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        required=True,
        help="The name of the schema.",
    )
    parser.add_argument(
        "--new-schema-file",
        type=str,
        help="Path to the database schema to bootstrap.",
    )
    parser.add_argument(
        "--skip-persisting-blueprint",
        action="store_true",
        help="Set this flag to avoid persisting the blueprint. "
        "Only meant to be used if you know what you are doing!",
    )
    parser.add_argument(
        "--engines", nargs="+", default=["aurora", "redshift", "athena"]
    )
    parser.set_defaults(admin_action=alter_schema)


async def alter_schema_impl(args):
    # 1. Load the config.
    config = ConfigFile.load(args.config_file)
    assets = AssetManager(config)
    blueprint_mgr = BlueprintManager(config, assets, args.schema_name)
    await blueprint_mgr.load()
    current_blueprint = blueprint_mgr.get_blueprint()

    # 2. Load and validate the user-provided schema.
    user = UserProvidedBlueprint.load_from_yaml_file(args.schema_file)
    user.validate()

    # 3. Get the bootstrapped blueprint.
    altered_blueprint = bootstrap_blueprint(user)

    # This alter schema is primitive for now (only to support experiments). It
    # only adds tables that are missing from the current blueprint.

    # 4. Connect to the engines.
    engines_filter = {Engine.from_str(engine_str) for engine_str in args.engines}
    cxns = EngineConnections.connect_sync(
        config,
        blueprint_mgr.get_directory(),
        schema_name=args.schema_name,
        autocommit=False,
        specific_engines=engines_filter,
    )

    # 5. Figure out which tables are new. These will be created.
    existing_tables = {table.name for table in current_blueprint.tables()}
    tables_to_create = {
        table.name for table in altered_blueprint if table.name not in existing_tables
    }

    # 6. Set up the new tables.
    sql_gen = TableSqlGenerator(config, altered_blueprint)

    for table in altered_blueprint.tables():
        if table.name not in tables_to_create:
            continue

        table_locations = altered_blueprint.get_table_locations(table.name)
        for location in table_locations:
            if location not in engines_filter:
                logger.info(
                    "Skipping creating '%s' on %s because the engine was not "
                    "specified using --engines.",
                    table.name,
                    location,
                )
                continue
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

    # 7. Update the extraction progress table.
    if Engine.Aurora in engines_filter:
        for table_name in tables_to_create:
            queries, db_type = sql_gen.generate_extraction_progress_init(table_name)
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

    # 10. Install the required extensions (needed for data extraction).
    if Engine.Aurora in engines_filter:
        aurora = cxns.get_connection(Engine.Aurora)
        cursor = aurora.cursor_sync()
        cursor.execute_sync("CREATE EXTENSION IF NOT EXISTS vector")
        cursor.commit_sync()

    # 11. Persist the data blueprint.
    if not args.skip_persisting_blueprint:
        merged_tables = current_blueprint.tables().copy()
        merged_table_locations = current_blueprint.table_locations().copy()

        # Append the new table metadata to the blueprint.
        for table_name in tables_to_create:
            merged_tables.append(altered_blueprint.get_table(table_name))
            merged_table_locations[table_name] = altered_blueprint.get_table_locations(
                table_name
            )

        merged_blueprint = Blueprint(
            current_blueprint.schema_name,
            merged_tables,
            merged_table_locations,
            current_blueprint.aurora_provisioning(),
            current_blueprint.redshift_provisioning(),
            current_blueprint.get_routing_policy(),
        )
        blueprint_mgr.force_new_blueprint_sync(merged_blueprint, score=None)

    logger.info("Done!")


# This method is called by `brad.exec.admin.main`.
def alter_schema(args):
    asyncio.run(alter_schema_impl(args))
