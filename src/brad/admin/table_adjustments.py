import asyncio
import logging
from typing import Awaitable, List

from brad.asset_manager import AssetManager
from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.provisioning.directory import Directory
from brad.blueprint.blueprint import Blueprint

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
    parser.set_defaults(admin_action=table_adjustments)


async def table_adjustments_impl(args) -> None:
    # 1. Load the config, blueprint, and provisioning.
    config = ConfigFile.load_from_physical_config(phys_config=args.physical_config_file)
    assets = AssetManager(config)

    blueprint_mgr = BlueprintManager(config, assets, args.schema_name)
    await blueprint_mgr.load()
    blueprint = blueprint_mgr.get_blueprint()

    directory = Directory(config)
    await directory.refresh()

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
        pass

    else:
        logger.error("Unknown action %s", args.action)

    logger.info("Done.")


# This method is called by `brad.exec.admin.main`.
def table_adjustments(args):
    asyncio.run(table_adjustments_impl(args))
