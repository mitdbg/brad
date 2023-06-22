import logging
import pyodbc

from brad.asset_manager import AssetManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.server.blueprint_manager import BlueprintManager
from brad.server.engine_connections import EngineConnections

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
        help="The name of the schema to drop.",
    )
    parser.set_defaults(admin_action=drop_schema)


# This method is called by `brad.exec.admin.main`.
def drop_schema(args):
    # 1. Load the config.
    config = ConfigFile(args.config_file)

    # 2. Delete the persisted data blueprint, if it exists.
    assets = AssetManager(config)
    data_blueprint_mgr = BlueprintManager(assets, args.schema_name)
    data_blueprint_mgr.delete_sync()

    # 3. Connect to the underlying engines without an explicit database.
    cxns = EngineConnections.connect_sync(config, autocommit=True)
    redshift = cxns.get_connection(Engine.Redshift).cursor()
    aurora = cxns.get_connection(Engine.Aurora).cursor()
    athena = cxns.get_connection(Engine.Athena).cursor()

    # 4. Drop the underlying "databases" if they exist.
    athena.execute("DROP DATABASE IF EXISTS {} CASCADE".format(args.schema_name))
    aurora.execute("DROP DATABASE IF EXISTS {}".format(args.schema_name))
    try:
        redshift.execute("DROP DATABASE {}".format(args.schema_name))
    except pyodbc.Error:
        # Ignore the error if a database does not exist.
        logger.exception("Exception when dropping Redshift database.")

    logger.info("Done!")
