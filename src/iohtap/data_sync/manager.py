import logging
import pyodbc

from iohtap.config.dbtype import DBType
from iohtap.config.file import ConfigFile
from iohtap.config.schema import Schema, Table, Column
from iohtap.config.strings import (
    AURORA_EXTRACT_PROGRESS_TABLE_NAME,
    shadow_table_name,
    imported_staging_table_name,
    imported_shadow_staging_table_name,
)
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

_UPDATE_EXTRACT_PROGRESS_BOTH = "UPDATE {} SET next_extract_seq = ?, next_shadow_extract_seq = ? WHERE table_name = ?".format(
    AURORA_EXTRACT_PROGRESS_TABLE_NAME
)
_UPDATE_EXTRACT_PROGRESS_SHADOW = (
    "UPDATE {} SET next_shadow_extract_seq = ? WHERE table_name = ?".format(
        AURORA_EXTRACT_PROGRESS_TABLE_NAME
    )
)
_UPDATE_EXTRACT_PROGRESS_NON_SHADOW = (
    "UPDATE {} SET next_extract_seq = ? WHERE table_name = ?".format(
        AURORA_EXTRACT_PROGRESS_TABLE_NAME
    )
)
_DELETE_FROM_SHADOW_STAGING = "DELETE FROM {shadow_staging_table} WHERE iohtap_seq >= {lower_bound} AND iohtap_seq <= {upper_bound}"

_REDSHIFT_CREATE_STAGING_TABLE = (
    "CREATE TEMPORARY TABLE {staging_table} LIKE {base_table}"
)
_REDSHIFT_CREATE_SHADOW_STAGING_TABLE = (
    "CREATE TEMPORARY TABLE {shadow_staging_table} ({pkey_cols})"
)
_REDSHIFT_IMPORT_COMMAND = "COPY {dest_table} FROM '{s3_file_path}' IAM_ROLE '{s3_iam_role}' REGION '{s3_region}'"
_REDSHIFT_DELETE_COMMAND = (
    "DELETE FROM {main_table} USING {staging_table} WHERE {conditions}"
)
_REDSHIFT_INSERT_COMMAND = "INSERT INTO {dest_table} SELECT * FROM {staging_table}"

_ATHENA_CREATE_STAGING_TABLE = """
    CREATE EXTERNAL TABLE {staging_table} ({columns})
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION '{s3_location}'
"""
_ATHENA_MERGE_COMMAND = """
    MERGE INTO {main_table} AS t
    USING (
        SELECT
        {pkey_cols},
        {other_cols},
        0 AS iohtap_is_delete
        FROM {staging_table}
        UNION ALL
        SELECT
        {pkey_cols},
        {other_cols_as_null},
        1 AS iohtap_is_delete
        FROM {shadow_staging_table}
    ) AS s
    ON {merge_cond}
    WHEN MATCHED AND s.iohtap_is_delete = 1
        THEN DELETE
    WHEN MATCHED AND s.iohtap_is_delete != 1
        THEN UPDATE SET ({update_cols})
    WHEN NOT MATCHED
        THEN INSERT VALUES ({insert_cols});
"""

_MAX_SEQ = 0xFFFFFFFF_FFFFFFFF


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
        # NOTE: This implementation assumes that at most one sync is running at
        # any time. Overall this is a reasonable assumption. However we may want
        # to break down the extract and import steps so that the imports into
        # Redshift and Athena can run concurrently.
        logger.debug("Starting data sync...")
        for table in self._schema.tables:
            self._sync_table(table)
        logger.debug("Sync complete.")

    def _sync_table(self, table: Table):
        aurora = self._aurora.cursor()
        redshift = self._dbs.get_connection(DBType.Redshift).cursor()
        athena = self._dbs.get_connection(DBType.Athena).cursor()

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
                max_extract_seq = _MAX_SEQ
            else:
                max_extract_seq = row[0]

            aurora.execute(
                _GET_MAX_EXTRACT_TEMPLATE.format(table_name=shadow_table_name(table))
            )
            row = aurora.fetchone()
            if row is None:
                # The scenario when the table is empty.
                # Ideally we should be using the DBMS' max value for BIGSERIAL.
                max_shadow_extract_seq = _MAX_SEQ
            else:
                max_shadow_extract_seq = row[0]

            # 2. Export changes to S3.
            extract_main_query = _EXTRACT_FROM_MAIN_TEMPLATE.format(
                table_cols=Column.comma_separated_names(table.columns),
                main_table=table.name,
                lower_bound=next_extract_seq,
                upper_bound=max_extract_seq,
            )
            extract_shadow_query = _EXTRACT_FROM_SHADOW_TEMPLATE.format(
                pkey_cols=Column.comma_separated_names(table.primary_key),
                main_table=shadow_table_name(table),
                lower_bound=next_shadow_extract_seq,
                upper_bound=max_shadow_extract_seq,
            )
            extract_main = _EXTRACT_S3_TEMPLATE.format(
                query=extract_main_query,
                s3_bucket=self._config.s3_extract_bucket,
                s3_file_path=self._get_s3_main_table_path(table),
                s3_region=self._config.s3_extract_region,
            )
            extract_shadow = _EXTRACT_S3_TEMPLATE.format(
                query=extract_shadow_query,
                s3_bucket=self._config.s3_extract_bucket,
                s3_file_path=self._get_s3_shadow_table_path(table),
                s3_region=self._config.s3_extract_region,
            )
            logger.debug("Running main export query: %s", extract_main)
            logger.debug("Running shadow export query: %s", extract_shadow)
            aurora.execute(extract_main)
            aurora.execute(extract_shadow)

            # 3. Import into Redshift and run a merge.
            # 3. a) Create staging tables.
            create_redshift_staging = _REDSHIFT_CREATE_STAGING_TABLE.format(
                staging_table=imported_staging_table_name(table), base_table=table.name
            )
            create_redshift_shadow_staging = (
                _REDSHIFT_CREATE_SHADOW_STAGING_TABLE.format(
                    shadow_staging_table=imported_shadow_staging_table_name(table),
                    pkey_cols=Column.comma_separated_names_and_types(
                        table.primary_key, DBType.Redshift
                    ),
                )
            )
            redshift.execute(create_redshift_staging)
            redshift.execute(create_redshift_shadow_staging)

            # 3. b) Import into the staging tables.
            import_redshift_staging = _REDSHIFT_IMPORT_COMMAND.format(
                dest_table=imported_staging_table_name(table),
                s3_file_path="s3://{}/{}".format(
                    self._config.s3_extract_bucket, self._get_s3_main_table_path(table)
                ),
                s3_iam_role=self._config.redshift_s3_iam_role,
                s3_region=self._config.s3_extract_region,
            )
            import_redshift_shadow_staging = _REDSHIFT_IMPORT_COMMAND.format(
                dest_table=imported_shadow_staging_table_name(table),
                s3_file_path="s3://{}/{}".format(
                    self._config.s3_extract_bucket,
                    self._get_s3_shadow_table_path(table),
                ),
                s3_iam_role=self._config.redshift_s3_iam_role,
                s3_region=self._config.s3_extract_region,
            )
            redshift.execute(import_redshift_staging)
            redshift.execute(import_redshift_shadow_staging)

            # 3. c) Delete updated and deleted rows from the main table.
            delete_using_redshift_staging = _REDSHIFT_DELETE_COMMAND.format(
                main_table=table.name,
                staging_table=imported_staging_table_name(table),
                conditions=self._generate_redshift_delete_conditions(
                    table, for_shadow_table=False
                ),
            )
            delete_using_redshift_shadow_staging = _REDSHIFT_DELETE_COMMAND.format(
                main_table=table.name,
                staging_table=imported_shadow_staging_table_name(table),
                conditions=self._generate_redshift_delete_conditions(
                    table, for_shadow_table=True
                ),
            )
            redshift.execute(delete_using_redshift_staging)
            redshift.execute(delete_using_redshift_shadow_staging)

            # 3. d) Insert new (and updated) rows.
            insert_using_redshift_staging = _REDSHIFT_INSERT_COMMAND.format(
                dest_table=table.name, staging_table=imported_staging_table_name(table)
            )
            redshift.execute(insert_using_redshift_staging)

            # 3. e) Commit and clean up.
            redshift.commit()
            redshift.execute("DROP TABLE {}".format(imported_staging_table_name(table)))
            redshift.execute(
                "DROP TABLE {}".format(imported_shadow_staging_table_name(table))
            )
            redshift.commit()

            # 4. Run a merge in Athena.
            # 4. a) Create staging tables.
            create_athena_staging = _ATHENA_CREATE_STAGING_TABLE.format(
                staging_table=imported_staging_table_name(table),
                columns=Column.comma_separated_names_and_types(
                    table.columns, DBType.Athena
                ),
                s3_location="s3://{}/{}".format(
                    self._config.s3_extract_bucket,
                    self._get_s3_main_table_path(table, include_file=False),
                ),
            )
            create_athena_shadow_staging = _ATHENA_CREATE_STAGING_TABLE.format(
                staging_table=imported_shadow_staging_table_name(table),
                columns=Column.comma_separated_names_and_types(
                    table.primary_key, DBType.Athena
                ),
                s3_location="s3://{}/{}".format(
                    self._config.s3_extract_bucket,
                    self._get_s3_shadow_table_path(table, include_file=False),
                ),
            )
            athena.execute(create_athena_staging)
            athena.execute(create_athena_shadow_staging)

            # 4. b) Run the merge.
            athena_merge_query = self._generate_athena_merge_query(table)
            athena.execute(athena_merge_query)

            # 4. c) Delete the staging tables (this deletes the schemas only, we
            # need to run an S3 object delete to actually delete the data).
            athena.execute("DROP TABLE {}".format(imported_staging_table_name(table)))
            athena.execute(
                "DROP TABLE {}".format(imported_shadow_staging_table_name(table))
            )

            # 5. Commit new next_extract_seq values and remove extracted rows from the shadow table.
            # NOTE: If any of the max extract sequence values are `_MAX_SEQ`, it
            # indicates that there were no values to extract.
            if max_shadow_extract_seq != _MAX_SEQ:
                aurora_delete_shadow = _DELETE_FROM_SHADOW_STAGING.format(
                    shadow_staging_table=imported_shadow_staging_table_name(table),
                    lower_bound=next_shadow_extract_seq,
                    upper_bound=max_shadow_extract_seq,
                )
                aurora.execute(aurora_delete_shadow)

            # Make sure we start at the right sequence number the next time we run an extraction.
            if max_extract_seq != _MAX_SEQ and max_shadow_extract_seq != _MAX_SEQ:
                aurora.execute(
                    _UPDATE_EXTRACT_PROGRESS_BOTH,
                    max_extract_seq + 1,
                    max_shadow_extract_seq + 1,
                    table.name,
                )
            elif max_extract_seq != _MAX_SEQ:
                aurora.execute(
                    _UPDATE_EXTRACT_PROGRESS_NON_SHADOW,
                    max_extract_seq + 1,
                    table.name,
                )
            elif max_shadow_extract_seq != _MAX_SEQ:
                aurora.execute(
                    _UPDATE_EXTRACT_PROGRESS_SHADOW,
                    max_shadow_extract_seq + 1,
                    table.name,
                )
            else:
                assert False

            # Indicate that the sync has completed.
            aurora.commit()

            # Ideally we also delete the temporary files (via S3). It's OK if we
            # do not delete them for now because they will be overwritten by the
            # next sync.

        except:  # pylint: disable=bare-except
            logger.exception("Encountered an exception when syncing data.")
            aurora.rollback()

    def _get_s3_main_table_path(self, table: Table, include_file: bool = True) -> str:
        prefix = "{}/{}/main".format(self._config.s3_extract_path, table.name)
        return prefix + "/table.tbl" if include_file else prefix

    def _get_s3_shadow_table_path(self, table: Table, include_file: bool = True) -> str:
        prefix = "{}/{}/shadow".format(self._config.s3_extract_path, table.name)
        return prefix + "/table.tbl" if include_file else prefix

    def _generate_redshift_delete_conditions(
        self, table: Table, for_shadow_table: bool
    ) -> str:
        conditions = []
        for col in table.primary_key:
            conditions.append(
                "{main_table}.{col_name} = {staging_table}.{col_name}".format(
                    main_table=table.name,
                    staging_table=imported_staging_table_name(table)
                    if not for_shadow_table
                    else imported_shadow_staging_table_name(table),
                    col_name=col.name,
                )
            )
        return " AND ".join(conditions)

    def _generate_athena_merge_query(self, table: Table) -> str:
        pkey_cols = Column.comma_separated_names(table.primary_key)
        non_primary_cols = list(filter(lambda c: not c.is_primary, table.columns))
        other_cols = Column.comma_separated_names(non_primary_cols)
        other_cols_as_null = ", ".join(
            map(lambda c: "NULL AS {}".format(c.name), non_primary_cols)
        )

        # Match rows by primary key.
        merge_conds = []
        for col in table.primary_key:
            merge_conds.append("t.{col_name} = s.{col_name}".format(col_name=col.name))
        merge_cond = " AND ".join(merge_conds)

        # Update row by setting it to the values in the staging table.
        update_cols_list = []
        for col in table.columns:
            update_cols_list.append(
                "{col_name} = s.{col_name}".format(col_name=col.name)
            )
        update_cols = ", ".join(update_cols_list)

        # Insert all columns.
        insert_cols = Column.comma_separated_names(table.columns)

        return _ATHENA_MERGE_COMMAND.format(
            pkey_cols=pkey_cols,
            other_cols=other_cols,
            other_cols_as_null=other_cols_as_null,
            merge_cond=merge_cond,
            update_cols=update_cols,
            insert_cols=insert_cols,
            main_table=table.name,
            staging_table=imported_staging_table_name(table),
            shadow_staging_table=imported_shadow_staging_table_name(table),
        )
