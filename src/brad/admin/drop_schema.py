import logging
import pyodbc

from brad.config.dbtype import DBType
from brad.config.file import ConfigFile
from brad.server.data_blueprint_manager import DataBlueprintManager
from brad.server.engine_connections import EngineConnections

logger = logging.getLogger(__name__)


# This method is called by `brad.exec.admin.main`.
def drop_schema(args):
    # 1. Load the config.
    config = ConfigFile(args.config_file)

    # 2. Delete the persisted data blueprint, if it exists.
    data_blueprint_mgr = DataBlueprintManager(config, args.drop_schema_name)
    data_blueprint_mgr.delete_sync()

    # 3. Connect to the underlying engines without an explicit database.
    cxns = EngineConnections.connect_sync(config, autocommit=True)
    redshift = cxns.get_connection(DBType.Redshift).cursor()
    aurora = cxns.get_connection(DBType.Aurora).cursor()
    athena = cxns.get_connection(DBType.Athena).cursor()

    # 4. Drop the underlying "databases" if they exist.
    athena.execute("DROP DATABASE IF EXISTS {} CASCADE".format(args.drop_schema_name))
    aurora.execute("DROP DATABASE IF EXISTS {}".format(args.drop_schema_name))
    try:
        redshift.execute("DROP DATABASE {}".format(args.drop_schema_name))
    except pyodbc.Error:
        # Ignore the error if a database does not exist.
        logger.exception("Exception when dropping Redshift database.")

    logger.info("Done!")
