import json
import logging
import pyodbc

from brad.asset_manager import AssetManager
from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.front_end.engine_connections import EngineConnections

logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "run_on", help="Run a query or command on a specific engine."
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
        help="The name of the schema to drop.",
    )
    parser.add_argument(
        "engine",
        type=str,
        help="The engine to run the query/command on.",
    )
    parser.add_argument("query_or_command", type=str, help="The query/command to run.")
    parser.set_defaults(admin_action=run_on)


# This method is called by `brad.exec.admin.main`.
def run_on(args):
    # 1. Load the config and blueprint.
    engine = Engine.from_str(args.engine)
    config = ConfigFile(args.config_file)
    assets = AssetManager(config)
    blueprint_mgr = BlueprintManager(config, assets, args.schema_name)
    blueprint_mgr.load_sync()

    # 2. Connect to the underlying engines without an explicit database.
    cxns = EngineConnections.connect_sync(
        config,
        blueprint_mgr.get_directory(),
        schema_name=args.schema_name,
        specific_engines={engine},
        autocommit=True,
    )

    try:
        conn = cxns.get_connection(engine)
        cursor = conn.cursor_sync()
        cursor.execute_sync(args.query_or_command)
        results = cursor.fetchall_sync()

        print(json.dumps(results, indent=2, default=str))
    except pyodbc.ProgrammingError:
        print("No rows produced.")
    finally:
        cxns.close_sync()
