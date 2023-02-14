from iohtap.config.dbtype import DBType
from iohtap.config.schema import Schema
from iohtap.config.strings import (
    delete_trigger_function_name,
    shadow_table_name,
    aurora_extract_progress_table_name,
)
from iohtap.config.extraction import ExtractionStrategy
from iohtap.config.file import ConfigFile
from iohtap.server.db_connection_manager import DBConnectionManager


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
        drop_main_table = drop_table_template.format(table.name)

        redshift.execute(drop_main_table)
        athena.execute(drop_main_table)

        # Triggers and indexes are automatically dropped.
        aurora.execute(drop_table_template.format(shadow_table_name(table)))
        aurora.execute(drop_main_table)
        aurora.execute(
            drop_trigger_fn_template.format(delete_trigger_function_name(table))
        )

    aurora.execute(drop_table_template.format(aurora_extract_progress_table_name()))

    # 5. Commit the changes.
    aurora.commit()
    redshift.commit()
    # Athena does not support the notion of committing a transaction.
