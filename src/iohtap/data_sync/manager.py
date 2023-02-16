import logging
import pyodbc
import sys

from iohtap.config.dbtype import DBType
from iohtap.config.file import ConfigFile
from iohtap.config.schema import Schema, Table
from iohtap.config.strings import AURORA_EXTRACT_PROGRESS_TABLE_NAME, shadow_table_name
from iohtap.server.db_connection_manager import DBConnectionManager

logger = logging.getLogger(__name__)

_GET_NEXT_EXTRACT = "SELECT next_extract_seq, next_shadow_extract_seq FROM {} WHERE table_name = ?".format(
    AURORA_EXTRACT_PROGRESS_TABLE_NAME
)
_GET_MAX_EXTRACT_TEMPLATE = "SELECT MAX(iohtap_seq) FROM {table_name}"

_EXTRACT_S3_TEMPLATE = """
SELECT * from aws_s3.query_export_to_s3(
    '{query}',
    aws_commons.create_s3_uri('{s3_bucket}', '{s3_file_path}', '{s3_region}'),
    options :='FORMAT text, DELIMITER ''|'''
);
"""
_EXTRACT_FROM_MAIN_TEMPLATE = "SELECT {table_cols} FROM {main_table} WHERE iohtap_seq >= {lower_bound} AND iohtap_seq <= {upper_bound}"
_EXTRACT_FROM_SHADOW_TEMPLATE = "SELECT {pkey_cols} FROM {shadow_table} WHERE iohtap_seq >= {lower_bound} AND iohtap_seq <= {upper_bound}"


class DataSyncManager:
    def __init__(self, config: ConfigFile, schema: Schema, dbs: DBConnectionManager):
        self._config = config
        self._schema = schema
        self._dbs = dbs

        # This class maintains its own connection to Aurora because it has
        # different isolation level requirements.
        self._aurora = pyodbc.connect(config.get_odbc_connection_string(DBType.Aurora))
        self._aurora.execute(
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE"
        )

    def run_sync(self):
        logger.debug("Starting data sync...")
        for table in self._schema.tables:
            self._sync_table(table)
        logger.debug("Sync complete.")

    def _sync_table(self, table: Table):
        aurora = self._aurora.cursor()
        try:
            logger.debug("Starting sync on '%s'...", table.name)

            # 1. Get data range to extract.
            #  - Select `next_extract_seq` values from the extraction progress table (lower bound)
            #  - Select current max sequence values from the main and shadow tables (upper bound)
            aurora.execute(_GET_NEXT_EXTRACT, table.name)
            row = aurora.fetchone()
            assert row is not None
            next_extract_seq, next_shadow_extract_seq = row

            aurora.execute(_GET_MAX_EXTRACT_TEMPLATE.format(table_name=table.name))
            row = aurora.fetchone()
            if row is None:
                # The scenario when the table is empty.
                # Ideally we should be using the DBMS' max value for BIGSERIAL.
                max_extract_seq = sys.maxint
            else:
                max_extract_seq = row[0]

            aurora.execute(
                _GET_MAX_EXTRACT_TEMPLATE.format(table_name=shadow_table_name(table))
            )
            row = aurora.fetchone()
            if row is None:
                # The scenario when the table is empty.
                # Ideally we should be using the DBMS' max value for BIGSERIAL.
                max_shadow_extract_seq = sys.maxint
            else:
                max_shadow_extract_seq = row[0]

            # 2. Export changes to S3.
            extract_main_query = _EXTRACT_FROM_MAIN_TEMPLATE.format(
                table_cols=", ".join(map(lambda col: col.name, table.columns)),
                main_table=table.name,
                lower_bound=next_extract_seq,
                upper_bound=max_extract_seq,
            )
            extract_shadow_query = _EXTRACT_FROM_SHADOW_TEMPLATE.format(
                pkey_cols=", ".join(map(lambda col: col.name, table.primary_key)),
                main_table=shadow_table_name(table),
                lower_bound=next_shadow_extract_seq,
                upper_bound=max_shadow_extract_seq,
            )
            extract_main = _EXTRACT_S3_TEMPLATE.format(
                query=extract_main_query,
                s3_bucket=self._config.s3_extract_bucket,
                s3_file_path="{}/{}/main/table".format(self._config.s3_extract_path, table.name),
                s3_region=self._config.s3_extract_region,
            )
            extract_shadow = _EXTRACT_S3_TEMPLATE.format(
                query=extract_shadow_query,
                s3_bucket=self._config.s3_extract_bucket,
                s3_file_path="{}/{}/shadow/table".format(self._config.s3_extract_path, table.name),
                s3_region=self._config.s3_extract_region,
            )
            logger.debug("Running main export query: %s", extract_main)
            logger.debug("Running shadow export query: %s", extract_shadow)
            aurora.execute(extract_main)
            aurora.execute(extract_shadow)

            # 3. Import into Redshift and run a merge.
            # 4. Run a merge in Athena.
            # 5. Commit new next_extract_seq values and remove extracted rows from the shadow table.
            # 6. Ideally we also delete the temporary files.

        except:
            aurora.rollback()
