import asyncio
import logging

from brad.asset_manager import AssetManager
from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.blueprint.blueprint import Blueprint
from brad.blueprint.sql_gen.table import TableSqlGenerator
from brad.front_end.engine_connections import EngineConnections

logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "table_adjustments",
        help="Used to manually modify the physical tables in BRAD's underlying infrastructure.",
    )
    parser.add_argument(
        "--physical-config-file",
        type=str,
        required=True,
        help="Path to BRAD's physical configuration file.",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        required=True,
        help="The schema name to use.",
    )
    parser.add_argument(
        "action",
        type=str,
        help="The action to run {remove_blueprint_table, rename_table}.",
    )
    parser.add_argument(
        "--table-name", type=str, help="The name of the table.", required=True
    )
    parser.add_argument("--engines", type=str, nargs="+", help="The engines involved.")
    parser.add_argument(
        "--new-table-name", type=str, help="The new table name, when applicable."
    )
    parser.set_defaults(admin_action=table_adjustments)


async def table_adjustments_impl(args) -> None:
    # 1. Load the config, blueprint, and provisioning.
    config = ConfigFile.load_from_physical_config(phys_config=args.physical_config_file)
    assets = AssetManager(config)

    blueprint_mgr = BlueprintManager(config, assets, args.schema_name)
    await blueprint_mgr.load()
    blueprint = blueprint_mgr.get_blueprint()
    directory = blueprint_mgr.get_directory()

    if args.action == "remove_blueprint_table":
        # NOTE: This only removes the table from the blueprint. You need to
        # manually remove it from the physical engines (if appropriate).
        table_to_remove = args.table_name
        new_blueprint = Blueprint(
            schema_name=blueprint.schema_name(),
            table_schemas=[
                table for table in blueprint.tables() if table.name != table_to_remove
            ],
            table_locations={
                table_name: locations
                for table_name, locations in blueprint.table_locations().items()
                if table_name != table_to_remove
            },
            aurora_provisioning=blueprint.aurora_provisioning(),
            redshift_provisioning=blueprint.redshift_provisioning(),
            full_routing_policy=blueprint.get_routing_policy(),
        )
        blueprint_mgr.force_new_blueprint_sync(new_blueprint, score=None)

    elif args.action == "rename_table":
        engines = {Engine.from_str(engine_str) for engine_str in args.engines}
        connections = EngineConnections.connect_sync(
            config,
            directory,
            schema_name=args.schema_name,
            autocommit=False,
            specific_engines=engines,
        )
        sqlgen = TableSqlGenerator(config, blueprint)
        for engine in engines:
            table = blueprint.get_table(args.table_name)
            logger.info(
                "On %s: Renaming table %s to %s",
                str(engine),
                table.name,
                args.new_table_name,
            )
            statements, run_on = sqlgen.generate_rename_table_sql(
                table, engine, args.new_table_name
            )
            conn = connections.get_connection(run_on)
            cursor = conn.cursor_sync()
            for stmt in statements:
                cursor.execute_sync(stmt)
            cursor.commit_sync()

    else:
        logger.error("Unknown action %s", args.action)

    logger.info("Done.")


# This method is called by `brad.exec.admin.main`.
def table_adjustments(args):
    asyncio.run(table_adjustments_impl(args))
