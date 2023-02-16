import logging
import pyodbc

from iohtap.config.dbtype import DBType
from iohtap.config.file import ConfigFile
from iohtap.config.schema import Schema, Table, Column
from iohtap.config.strings import (
    shadow_table_name,
    imported_staging_table_name,
    imported_shadow_staging_table_name,
)
from iohtap.server.db_connection_manager import DBConnectionManager
from ._templates import (
    GET_NEXT_EXTRACT,
    GET_MAX_EXTRACT_TEMPLATE,
    EXTRACT_FROM_MAIN_TEMPLATE,
    EXTRACT_FROM_SHADOW_TEMPLATE,
    EXTRACT_S3_TEMPLATE,
    REDSHIFT_CREATE_STAGING_TABLE,
    REDSHIFT_CREATE_SHADOW_STAGING_TABLE,
    REDSHIFT_IMPORT_COMMAND,
    REDSHIFT_DELETE_COMMAND,
    REDSHIFT_INSERT_COMMAND,
    ATHENA_CREATE_STAGING_TABLE,
    ATHENA_MERGE_COMMAND,
    UPDATE_EXTRACT_PROGRESS_BOTH,
    UPDATE_EXTRACT_PROGRESS_NON_SHADOW,
    UPDATE_EXTRACT_PROGRESS_SHADOW,
    DELETE_FROM_SHADOW_STAGING,
)

logger = logging.getLogger(__name__)

_MAX_SEQ = 0xFFFFFFFF_FFFFFFFF


class _SyncContext:
    def __init__(self, table: Table):
        self.table = table

        self.next_extract_seq = -1
        self.max_extract_seq = -1

        self.next_shadow_extract_seq = -1
        self.max_shadow_extract_seq = -1

    def bounds_set(self) -> bool:
        return (
            self.next_extract_seq >= 0
            and self.max_extract_seq >= 0
            and self.next_shadow_extract_seq >= 0
            and self.max_shadow_extract_seq >= 0
        )

    def can_skip_sync(self) -> bool:
        # These extract sequence ranges are inclusive. If both ranges are empty,
        # there were no new writes since the last time the sync ran. So we can
        # safely skip the sync.
        return (
            # _MAX_SEQ means that the table(s) are empty.
            (
                self.next_extract_seq > self.max_extract_seq
                or self.max_extract_seq == _MAX_SEQ
            )
            and (
                self.next_shadow_extract_seq > self.max_shadow_extract_seq
                or self.max_shadow_extract_seq == _MAX_SEQ
            )
        )

    def should_advance_main_seq(self) -> bool:
        """
        Returns true when the extraction range is non-empty (and so the next
        extraction should start at a higher sequence number).
        """
        return (
            self.max_extract_seq != _MAX_SEQ
            and self.max_extract_seq >= self.next_extract_seq
        )

    def should_advance_shadow_seq(self) -> bool:
        """
        Returns true when the shadow table extraction range is non-empty (and so the next
        extraction should start at a higher sequence number).
        """
        return (
            self.max_shadow_extract_seq != _MAX_SEQ
            and self.max_shadow_extract_seq >= self.next_shadow_extract_seq
        )

    def __repr__(self) -> str:
        return "SyncContext(table={}, main_seq_range=[{}, {}], shadow_seq_range=[{}, {}])".format(
            self.table.name,
            self.next_extract_seq,
            self.max_extract_seq,
            self.next_shadow_extract_seq,
            self.max_shadow_extract_seq,
        )


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
            ctx = _SyncContext(table)

            # 1. Get data range to extract.
            self._fetch_and_set_sync_bounds(aurora, ctx)
            if not ctx.bounds_set():
                logger.error("Invalid SyncContext: %s", str(ctx))
                assert False
            if ctx.can_skip_sync():
                logger.debug(
                    "Skipping syncing '%s' because there are no new writes.", table.name
                )
                aurora.commit()
                return

            logger.debug("Syncing using context: %s", str(ctx))

            # 2. Export changes to S3.
            self._export_aurora_to_s3(aurora, ctx)

            # 3. Import into Redshift and run a merge.
            self._import_s3_to_redshift(redshift, ctx)

            # 4. Run a merge in Athena.
            self._merge_into_athena(athena, ctx)

            # 5. Commit new next_extract_seq values and remove extracted rows from the shadow table.
            self._complete_sync(aurora, ctx)
            logger.debug("Completed syncing '%s'", table.name)

        except:  # pylint: disable=bare-except
            logger.exception("Encountered an exception when syncing data.")
            aurora.rollback()

    def _fetch_and_set_sync_bounds(self, aurora, ctx: _SyncContext):
        #  - Select `next_extract_seq` values from the extraction progress table (lower bound)
        #  - Select current max sequence values from the main and shadow tables (upper bound)
        aurora.execute(GET_NEXT_EXTRACT, ctx.table.name)
        row = aurora.fetchone()
        assert row is not None
        ctx.next_extract_seq, ctx.next_shadow_extract_seq = row

        aurora.execute(GET_MAX_EXTRACT_TEMPLATE.format(table_name=ctx.table.name))
        row = aurora.fetchone()
        if row is None or row[0] is None:
            # The scenario when the table is empty.
            # Ideally we should be using the DBMS' max value for BIGSERIAL.
            ctx.max_extract_seq = _MAX_SEQ
        else:
            ctx.max_extract_seq = row[0]

        aurora.execute(
            GET_MAX_EXTRACT_TEMPLATE.format(table_name=shadow_table_name(ctx.table))
        )
        row = aurora.fetchone()
        if row is None or row[0] is None:
            # The scenario when the table is empty.
            # Ideally we should be using the DBMS' max value for BIGSERIAL.
            ctx.max_shadow_extract_seq = _MAX_SEQ
        else:
            ctx.max_shadow_extract_seq = row[0]

    def _export_aurora_to_s3(self, aurora, ctx: _SyncContext):
        extract_main_query = EXTRACT_FROM_MAIN_TEMPLATE.format(
            table_cols=Column.comma_separated_names(ctx.table.columns),
            main_table=ctx.table.name,
            lower_bound=ctx.next_extract_seq,
            upper_bound=ctx.max_extract_seq,
        )
        extract_shadow_query = EXTRACT_FROM_SHADOW_TEMPLATE.format(
            pkey_cols=Column.comma_separated_names(ctx.table.primary_key),
            shadow_table=shadow_table_name(ctx.table),
            lower_bound=ctx.next_shadow_extract_seq,
            upper_bound=ctx.max_shadow_extract_seq,
        )
        extract_main = EXTRACT_S3_TEMPLATE.format(
            query=extract_main_query,
            s3_bucket=self._config.s3_extract_bucket,
            s3_file_path=self._get_s3_main_table_path(ctx.table),
            s3_region=self._config.s3_extract_region,
        )
        extract_shadow = EXTRACT_S3_TEMPLATE.format(
            query=extract_shadow_query,
            s3_bucket=self._config.s3_extract_bucket,
            s3_file_path=self._get_s3_shadow_table_path(ctx.table),
            s3_region=self._config.s3_extract_region,
        )
        logger.debug("Running main export query: %s", extract_main)
        logger.debug("Running shadow export query: %s", extract_shadow)
        aurora.execute(extract_main)
        aurora.execute(extract_shadow)

    def _import_s3_to_redshift(self, redshift, ctx: _SyncContext):
        # a) Create staging tables.
        redshift.execute(
            "DROP TABLE IF EXISTS {}".format(imported_staging_table_name(ctx.table))
        )
        redshift.execute(
            "DROP TABLE IF EXISTS {}".format(
                imported_shadow_staging_table_name(ctx.table)
            )
        )
        create_redshift_staging = REDSHIFT_CREATE_STAGING_TABLE.format(
            staging_table=imported_staging_table_name(ctx.table),
            base_table=ctx.table.name,
        )
        create_redshift_shadow_staging = REDSHIFT_CREATE_SHADOW_STAGING_TABLE.format(
            shadow_staging_table=imported_shadow_staging_table_name(ctx.table),
            pkey_cols=Column.comma_separated_names_and_types(
                ctx.table.primary_key, DBType.Redshift
            ),
        )
        logger.debug("Running on Redshift: %s", create_redshift_staging)
        redshift.execute(create_redshift_staging)
        logger.debug("Running on Redshift: %s", create_redshift_shadow_staging)
        redshift.execute(create_redshift_shadow_staging)

        # b) Import into the staging tables.
        import_redshift_staging = REDSHIFT_IMPORT_COMMAND.format(
            dest_table=imported_staging_table_name(ctx.table),
            s3_file_path="s3://{}/{}".format(
                self._config.s3_extract_bucket, self._get_s3_main_table_path(ctx.table)
            ),
            s3_iam_role=self._config.redshift_s3_iam_role,
            s3_region=self._config.s3_extract_region,
        )
        import_redshift_shadow_staging = REDSHIFT_IMPORT_COMMAND.format(
            dest_table=imported_shadow_staging_table_name(ctx.table),
            s3_file_path="s3://{}/{}".format(
                self._config.s3_extract_bucket,
                self._get_s3_shadow_table_path(ctx.table),
            ),
            s3_iam_role=self._config.redshift_s3_iam_role,
            s3_region=self._config.s3_extract_region,
        )
        logger.debug("Running on Redshift: %s", import_redshift_staging)
        redshift.execute(import_redshift_staging)
        logger.debug("Running on Redshift: %s", import_redshift_shadow_staging)
        redshift.execute(import_redshift_shadow_staging)

        # c) Delete updated and deleted rows from the main table.
        delete_using_redshift_staging = REDSHIFT_DELETE_COMMAND.format(
            main_table=ctx.table.name,
            staging_table=imported_staging_table_name(ctx.table),
            conditions=self._generate_redshift_delete_conditions(
                ctx.table, for_shadow_table=False
            ),
        )
        delete_using_redshift_shadow_staging = REDSHIFT_DELETE_COMMAND.format(
            main_table=ctx.table.name,
            staging_table=imported_shadow_staging_table_name(ctx.table),
            conditions=self._generate_redshift_delete_conditions(
                ctx.table, for_shadow_table=True
            ),
        )
        logger.debug("Running on Redshift: %s", delete_using_redshift_staging)
        redshift.execute(delete_using_redshift_staging)
        logger.debug("Running on Redshift: %s", delete_using_redshift_shadow_staging)
        redshift.execute(delete_using_redshift_shadow_staging)

        # d) Insert new (and updated) rows.
        insert_using_redshift_staging = REDSHIFT_INSERT_COMMAND.format(
            dest_table=ctx.table.name,
            staging_table=imported_staging_table_name(ctx.table),
        )
        logger.debug("Running on Redshift: %s", insert_using_redshift_staging)
        redshift.execute(insert_using_redshift_staging)

        # 3. e) Commit and clean up.
        redshift.commit()
        redshift.execute("DROP TABLE {}".format(imported_staging_table_name(ctx.table)))
        redshift.execute(
            "DROP TABLE {}".format(imported_shadow_staging_table_name(ctx.table))
        )
        redshift.commit()

    def _merge_into_athena(self, athena, ctx: _SyncContext):
        # a) Create staging tables.
        athena.execute(
            "DROP TABLE IF EXISTS {}".format(imported_staging_table_name(ctx.table))
        )
        athena.execute(
            "DROP TABLE IF EXISTS {}".format(
                imported_shadow_staging_table_name(ctx.table)
            )
        )
        create_athena_staging = ATHENA_CREATE_STAGING_TABLE.format(
            staging_table=imported_staging_table_name(ctx.table),
            columns=Column.comma_separated_names_and_types(
                ctx.table.columns, DBType.Athena
            ),
            s3_location="s3://{}/{}".format(
                self._config.s3_extract_bucket,
                self._get_s3_main_table_path(ctx.table, include_file=False),
            ),
        )
        create_athena_shadow_staging = ATHENA_CREATE_STAGING_TABLE.format(
            staging_table=imported_shadow_staging_table_name(ctx.table),
            columns=Column.comma_separated_names_and_types(
                ctx.table.primary_key, DBType.Athena
            ),
            s3_location="s3://{}/{}".format(
                self._config.s3_extract_bucket,
                self._get_s3_shadow_table_path(ctx.table, include_file=False),
            ),
        )
        logger.debug("Running on Athena: %s", create_athena_staging)
        athena.execute(create_athena_staging)
        logger.debug("Running on Athena: %s", create_athena_shadow_staging)
        athena.execute(create_athena_shadow_staging)

        # b) Run the merge.
        athena_merge_query = self._generate_athena_merge_query(ctx.table)
        logger.debug("Running on Athena: %s", athena_merge_query)
        athena.execute(athena_merge_query)

        # c) Delete the staging tables (this deletes the schemas only, we
        # need to run an S3 object delete to actually delete the data).
        athena.execute("DROP TABLE {}".format(imported_staging_table_name(ctx.table)))
        athena.execute(
            "DROP TABLE {}".format(imported_shadow_staging_table_name(ctx.table))
        )

    def _complete_sync(self, aurora, ctx: _SyncContext):
        # NOTE: If any of the max extract sequence values are `_MAX_SEQ`, it
        # indicates that there were no values to extract.
        if ctx.max_shadow_extract_seq != _MAX_SEQ:
            aurora_delete_shadow = DELETE_FROM_SHADOW_STAGING.format(
                shadow_staging_table=shadow_table_name(ctx.table),
                lower_bound=ctx.next_shadow_extract_seq,
                upper_bound=ctx.max_shadow_extract_seq,
            )
            aurora.execute(aurora_delete_shadow)

        # Make sure we start at the right sequence number the next time we run an extraction.
        # We skip updating the "next sequence number" when the extraction range is empty.
        if ctx.should_advance_main_seq() and ctx.should_advance_shadow_seq():
            logger.debug(
                "Setting next main sync seq: %d, next shadow sync seq: %d",
                ctx.max_extract_seq + 1,
                ctx.max_shadow_extract_seq + 1,
            )
            aurora.execute(
                UPDATE_EXTRACT_PROGRESS_BOTH,
                ctx.max_extract_seq + 1,
                ctx.max_shadow_extract_seq + 1,
                ctx.table.name,
            )
        elif ctx.should_advance_main_seq():
            logger.debug("Setting next main sync seq: %d", ctx.max_extract_seq + 1)
            aurora.execute(
                UPDATE_EXTRACT_PROGRESS_NON_SHADOW,
                ctx.max_extract_seq + 1,
                ctx.table.name,
            )
        elif ctx.should_advance_shadow_seq():
            logger.debug(
                "Setting next shadow sync seq: %d", ctx.max_shadow_extract_seq + 1
            )
            aurora.execute(
                UPDATE_EXTRACT_PROGRESS_SHADOW,
                ctx.max_shadow_extract_seq + 1,
                ctx.table.name,
            )
        else:
            # This case should not happen - we skip the sync if both ranges are empty.
            assert False

        # Indicate that the sync has completed.
        aurora.commit()

        # Ideally we also delete the temporary files (via S3). It's OK if we
        # do not delete them for now because they will be overwritten by the
        # next sync.

    def _get_s3_main_table_path(self, table: Table, include_file: bool = True) -> str:
        prefix = "{}{}/main/".format(self._config.s3_extract_path, table.name)
        return prefix + "table.tbl" if include_file else prefix

    def _get_s3_shadow_table_path(self, table: Table, include_file: bool = True) -> str:
        prefix = "{}{}/shadow/".format(self._config.s3_extract_path, table.name)
        return prefix + "table.tbl" if include_file else prefix

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
        insert_cols = ", ".join(map(lambda c: "s.{}".format(c.name), table.columns))

        return ATHENA_MERGE_COMMAND.format(
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
