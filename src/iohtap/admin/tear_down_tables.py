import logging

from iohtap.config.dbtype import DBType
from iohtap.config.schema import Schema
from iohtap.config.strings import (
    delete_trigger_function_name,
    shadow_table_name,
    AURORA_EXTRACT_PROGRESS_TABLE_NAME,
)
from iohtap.config.extraction import ExtractionStrategy
from iohtap.config.file import ConfigFile
from iohtap.server.db_connection_manager import DBConnectionManager

logger = logging.getLogger(__name__)


# This method is called by `iohtap.exec.admin.main`.
def tear_down_tables(args):
    # 1. Load the schema file.
    schema = Schema.load(args.schema_file)

    # 2. Load the config.
    config = ConfigFile(args.config_file)
    if config.extraction_strategy != ExtractionStrategy.SequenceTrigger:
        raise NotImplementedError(
            "Unsupported extraction strategy: {}".format(
                str(config.extraction_strategy)
            )
        )

    # 3. Connect to the underlying engines.
    cxns = DBConnectionManager(config)
    redshift = cxns.get_connection(DBType.Redshift).cursor()
    aurora = cxns.get_connection(DBType.Aurora).cursor()
    athena = cxns.get_connection(DBType.Athena).cursor()

    drop_table_template = "DROP TABLE {}"
    drop_trigger_fn_template = "DROP FUNCTION {}"

    # 4. Drop the tables.
    for table in schema.tables:
        logger.info("Deleting table '%s'...", table.name)
        drop_main_table = drop_table_template.format(table.name)

        logger.debug("Running on Redshift: %s", drop_main_table)
        redshift.execute(drop_main_table)
        logger.debug("Running on Athena: %s", drop_main_table)
        athena.execute(drop_main_table)

        # Triggers and indexes are automatically dropped.
        query = drop_table_template.format(shadow_table_name(table))
        logger.debug("Running on Aurora %s", query)
        aurora.execute(query)

        logger.debug("Running on Aurora: %s", drop_main_table)
        aurora.execute(drop_main_table)

        query = drop_trigger_fn_template.format(delete_trigger_function_name(table))
        logger.debug("Running on Aurora: %s", query)
        aurora.execute(query)

    query = drop_table_template.format(AURORA_EXTRACT_PROGRESS_TABLE_NAME)
    logger.debug("Running on Aurora: %s", query)
    aurora.execute(query)

    # 5. Commit the changes.
    aurora.commit()
    redshift.commit()
    # Athena does not support the notion of committing a transaction.

    logger.info("Done!")
