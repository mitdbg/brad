from typing import List

from iohtap.config.schema import Schema, Table
from iohtap.config.dbtype import DBType
from iohtap.config.file import ConfigFile
from iohtap.server.db_connection_manager import DBConnectionManager


# This method is called by `iohtap.exec.admin.main`.
def set_up_tables(args):
    # 1. Load the schema file.
    schema = Schema.load(args.schema_file)

    # 2. Load the config.
    config = ConfigFile(args.config_file)

    # 3. Connect to the underlying engines.
    cxns = DBConnectionManager(config)
    redshift = cxns.get_connection(DBType.Redshift).cursor()
    aurora = cxns.get_connection(DBType.Aurora).cursor()
    athena = cxns.get_connection(DBType.Athena).cursor()

    # 4. Set up the underlying tables.
    redshift_template = "CREATE TABLE {} ({}, PRIMARY KEY ({}));"
    athena_template = "CREATE TABLE {} ({}, PRIMARY KEY ({})) LOCATION '{}' TBLPROPERTIES ('table_type' = 'ICEBERG');"

    for table in schema.tables:
        _set_up_aurora_table(aurora, table, config)

        col_str = ", ".join(
            map(lambda c: "{} {}".format(c.name, c.data_type), table.columns)
        )
        primary_key_str = ", ".join(map(lambda c: c.name, table.primary_key))
        redshift.execute(redshift_template.format(table.name, col_str, primary_key_str))
        athena.execute(
            athena_template.format(
                table.name, col_str, primary_key_str, config.athena_s3_data_path
            )
        )

    aurora.commit()
    redshift.commit()
    # Athena does not support the notion of committing a transaction.


def _set_up_aurora_table(cursor, table: Table, config: ConfigFile):
    # TODO: This function is a bit more complex, as we need to create the extra extraction tables.
    aurora_extract_main_template = (
        "CREATE TABLE {} ({}, iohtap_seq BIGSERIAL, PRIMARY KEY ({}));"
    )
    aurora_extract_shadow_template = "CREATE TABLE {}_iohtap_shadow ({});"
