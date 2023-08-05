import asyncio
import logging

from brad.asset_manager import AssetManager
from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.front_end.engine_connections import EngineConnections
from brad.provisioning.directory import Directory
from brad.routing.policy import RoutingPolicy
from brad.routing.tree_based.forest_router import ForestRouter

logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser("drop_schema", help="Drop a schema from BRAD.")
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
        help="The name of the schema to drop.",
    )
    parser.set_defaults(admin_action=drop_schema)


# This method is called by `brad.exec.admin.main`.
def drop_schema(args):
    # 1. Load the config and blueprint.
    config = ConfigFile(args.config_file)

    # 2. Connect to the underlying engines without an explicit database.
    directory = Directory(config)
    asyncio.run(directory.refresh())
    cxns = EngineConnections.connect_sync(config, directory, autocommit=True)
    redshift = cxns.get_connection(Engine.Redshift).cursor_sync()
    aurora = cxns.get_connection(Engine.Aurora).cursor_sync()
    athena = cxns.get_connection(Engine.Athena).cursor_sync()

    logger.info("Starting the schema drop...")

    # 3. Drop the underlying "databases" if they exist.
    athena.execute_sync("DROP DATABASE IF EXISTS {} CASCADE".format(args.schema_name))
    aurora.execute_sync("DROP DATABASE IF EXISTS {}".format(args.schema_name))
    try:
        redshift.execute_sync("DROP DATABASE {}".format(args.schema_name))
    except:  # pylint: disable=bare-except
        # Ignore the error if a database does not exist.
        logger.exception("Exception when dropping Redshift database.")

    # 4. Drop any serialized routers.
    assets = AssetManager(config)
    ForestRouter.static_drop_model_sync(
        args.schema_name, RoutingPolicy.ForestTableSelectivity, assets
    )
    ForestRouter.static_drop_model_sync(
        args.schema_name, RoutingPolicy.ForestTablePresence, assets
    )

    # 5. Delete the persisted blueprint if it exists.
    blueprint_mgr = BlueprintManager(config, assets, args.schema_name)
    blueprint_mgr.delete_sync()

    logger.info("Done!")
