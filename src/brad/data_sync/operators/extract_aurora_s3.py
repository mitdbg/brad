import logging
from typing import Dict, Tuple

from .operator import Operator
from ._extract_aurora_s3_templates import (
    GET_NEXT_EXTRACT_TEMPLATE,
    GET_MAX_EXTRACT_TEMPLATE,
    EXTRACT_S3_TEMPLATE,
    EXTRACT_FROM_MAIN_TEMPLATE,
    EXTRACT_FROM_SHADOW_TEMPLATE,
    DELETE_FROM_SHADOW,
    UPDATE_EXTRACT_PROGRESS_BOTH,
    UPDATE_EXTRACT_PROGRESS_NON_SHADOW,
    UPDATE_EXTRACT_PROGRESS_SHADOW,
)
from brad.blueprint.sql_gen.table import comma_separated_column_names
from brad.config.strings import source_table_name, shadow_table_name
from brad.data_sync.execution.context import ExecutionContext

logger = logging.getLogger(__name__)

# Represents the largest possible sequence value (BIGSERIAL).
# Ideally we should be using the DBMS' value.
_MAX_SEQ = 0xFFFFFFFF_FFFFFFFF


class ExtractFromAuroraToS3(Operator):
    def __init__(self, tables: Dict[str, Tuple[str, str]]) -> None:
        super().__init__()
        self._to_extract = tables

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        # 1. Retrieve the sequence ranges for extraction.
        table_bounds = await self._get_table_extract_bounds(ctx)

        # 2. Remove tables that do not need to be extracted.
        can_skip = []
        for table_name, bounds in table_bounds.items():
            # Sanity check.
            assert bounds.bounds_set(), table_name
            if bounds.can_skip_sync():
                can_skip.append(table_name)
        for table_name in can_skip:
            logger.debug(
                "Skipping extracting '%s' because it has not experienced new writes.",
                table_name,
            )
            del table_bounds[table_name]

        # 3. Run the extraction.
        for table_name, bounds in table_bounds.items():
            await self._export_table_to_s3(ctx, table_name, bounds)

        # 4. Update the extraction progress tables.
        # We can safely resume the data sync from the extracted data saved in S3
        # if needed.
        for table_name, bounds in table_bounds.items():
            await self._complete_sync(ctx, table_name, bounds)

        cursor = await ctx.aurora()
        await cursor.commit()
        ctx.set_extracted_tables(list(table_bounds.keys()))

        return self

    async def _get_table_extract_bounds(
        self, ctx: ExecutionContext
    ) -> Dict[str, "_TableSyncBounds"]:
        cursor = await ctx.aurora()

        # 1. Retrieve the starting sequence values for extraction.
        q = GET_NEXT_EXTRACT_TEMPLATE.format(
            extract_tables=", ".join(self._to_extract.keys())
        )
        logger.debug("Executing on Aurora %s", q)
        await cursor.execute(q)

        # NOTE: A lower bound for the shadow table is not absolutely needed
        # because we run under strict serializable isolation (to ensure we
        # always extract a transactionally-consistent snapshot).

        # table_name: (main_bound, shadow_bound)
        table_bounds: Dict[str, _TableSyncBounds] = {}
        async for row in cursor:
            bounds = _TableSyncBounds()
            bounds.next_extract_seq = row[1]
            bounds.next_shadow_extract_seq = row[2]
            table_bounds[row[0]] = bounds

        # 2. Retrieve the current upper bounds for extraction.
        for table_name, bounds in table_bounds.items():
            # Main table.
            q = GET_MAX_EXTRACT_TEMPLATE.format(
                table_name=source_table_name(table_name)
            )
            logger.debug("Executing on Aurora %s", q)
            await cursor.execute(q)
            row = await cursor.fetchone()
            if row is None or row[0] is None:
                # The scenario when the table is empty.
                # Ideally we should be using the DBMS' max value for BIGSERIAL.
                bounds.max_extract_seq = _MAX_SEQ
            else:
                bounds.max_extract_seq = row[0]

            # Shadow table.
            q = GET_MAX_EXTRACT_TEMPLATE.format(
                table_name=shadow_table_name(table_name)
            )
            logger.debug("Executing on Aurora %s", q)
            await cursor.execute(q)
            row = await cursor.fetchone()
            if row is None or row[0] is None:
                # The scenario when the table is empty.
                # Ideally we should be using the DBMS' max value for BIGSERIAL.
                bounds.max_shadow_extract_seq = _MAX_SEQ
            else:
                bounds.max_shadow_extract_seq = row[0]

        return table_bounds

    async def _export_table_to_s3(
        self, ctx: ExecutionContext, table_name: str, bounds: "_TableSyncBounds"
    ):
        cursor = await ctx.aurora()

        blueprint = ctx.blueprint()
        table = blueprint.get_table(table_name)

        extract_main_query = EXTRACT_FROM_MAIN_TEMPLATE.format(
            table_cols=comma_separated_column_names(table.columns),
            main_table=source_table_name(table_name),
            lower_bound=bounds.next_extract_seq,
            upper_bound=bounds.max_extract_seq,
        )
        extract_shadow_query = EXTRACT_FROM_SHADOW_TEMPLATE.format(
            pkey_cols=comma_separated_column_names(table.primary_key),
            shadow_table=shadow_table_name(table_name),
            lower_bound=bounds.next_shadow_extract_seq,
            upper_bound=bounds.max_shadow_extract_seq,
        )
        extract_main = EXTRACT_S3_TEMPLATE.format(
            query=extract_main_query,
            s3_bucket=ctx.s3_bucket(),
            s3_region=ctx.s3_region(),
            s3_file_path=self._to_extract[table_name][0],
        )
        extract_shadow = EXTRACT_S3_TEMPLATE.format(
            query=extract_shadow_query,
            s3_bucket=ctx.s3_bucket(),
            s3_region=ctx.s3_region(),
            s3_file_path=self._to_extract[table_name][1],
        )
        logger.debug("Running main export query: %s", extract_main)
        logger.debug("Running shadow export query: %s", extract_shadow)
        await cursor.execute(extract_main)
        await cursor.execute(extract_shadow)

    async def _complete_sync(
        self, ctx: ExecutionContext, table_name: str, bounds: "_TableSyncBounds"
    ):
        cursor = await ctx.aurora()

        # NOTE: If any of the max extract sequence values are `_MAX_SEQ`, it
        # indicates that there were no values to extract.
        if bounds.max_shadow_extract_seq != _MAX_SEQ:
            aurora_delete_shadow = DELETE_FROM_SHADOW.format(
                shadow_table=shadow_table_name(table_name),
                lower_bound=bounds.next_shadow_extract_seq,
                upper_bound=bounds.max_shadow_extract_seq,
            )
            logger.debug("Running on Aurora: %s", aurora_delete_shadow)
            await cursor.execute(aurora_delete_shadow)

        # Make sure we start at the right sequence number the next time we run an extraction.
        # We skip updating the "next sequence number" when the extraction range is empty.
        if bounds.should_advance_main_seq() and bounds.should_advance_shadow_seq():
            next_main = bounds.max_extract_seq + 1
            next_shadow = bounds.max_shadow_extract_seq + 1
            logger.debug(
                "Setting next main sync seq: %d, next shadow sync seq: %d",
                next_main,
                next_shadow,
            )
            query = UPDATE_EXTRACT_PROGRESS_BOTH.format(
                table_name=table_name,
                next_main=next_main,
                next_shadow=next_shadow,
            )
            logger.debug("Running on Aurora: %s", query)
            await cursor.execute(query)

        elif bounds.should_advance_main_seq():
            next_main = bounds.max_extract_seq + 1
            logger.debug("Setting next main sync seq: %d", next_main)
            query = UPDATE_EXTRACT_PROGRESS_NON_SHADOW.format(
                table_name=table_name,
                next_main=next_main,
            )
            logger.debug("Running on Aurora: %s", query)
            await cursor.execute(query)

        elif bounds.should_advance_shadow_seq():
            next_shadow = bounds.max_shadow_extract_seq + 1
            logger.debug("Setting next shadow sync seq: %d", next_shadow)
            query = UPDATE_EXTRACT_PROGRESS_SHADOW.format(
                table_name=table_name,
                next_shadow=next_shadow,
            )
            logger.debug("Running on Aurora: %s", query)
            await cursor.execute(query)

        else:
            # This case should not happen - we skip the sync if both ranges are empty.
            assert False


class _TableSyncBounds:
    def __init__(self):
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
        return "TableSyncBounds(main_seq_range=[{}, {}], shadow_seq_range=[{}, {}])".format(
            self.next_extract_seq,
            self.max_extract_seq,
            self.next_shadow_extract_seq,
            self.max_shadow_extract_seq,
        )
