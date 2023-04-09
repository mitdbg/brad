import logging
from typing import Dict

from .operator import Operator
from ._extract_aurora_s3_templates import (
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
from brad.data_sync.execution.table_sync_bounds import TableSyncBounds, MAX_SEQ
from brad.data_sync.s3_path import S3Path

logger = logging.getLogger(__name__)


class ExtractFromAuroraToS3(Operator):
    def __init__(self, tables: Dict[str, "ExtractLocation"]) -> None:
        super().__init__()
        self._to_extract = tables

    def __repr__(self) -> str:
        return "".join(
            ["ExtractFromAuroraToS3(", ", ".join(self._to_extract.keys()), ")"]
        )

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        # 1. Retrieve the sequence ranges for extraction.
        table_bounds = ctx.table_sync_bounds()

        # 2. Verify that all tables to be extracted are in `table_bounds` and
        # are not to be skipped (we would have pruned these tables already).
        for table_name in self._to_extract.keys():
            assert table_name in table_bounds
            bounds = table_bounds[table_name]
            assert bounds.bounds_set()
            assert not bounds.can_skip_sync()

        # 3. Run the extraction.
        for table_name in self._to_extract.keys():
            await self._export_table_to_s3(ctx, table_name, table_bounds[table_name])

        # 4. Update the extraction progress tables.
        # We can safely resume the data sync from the extracted data saved in S3
        # if needed.
        for table_name in self._to_extract.keys():
            await self._complete_sync(ctx, table_name, table_bounds[table_name])

        cursor = await ctx.aurora()
        await cursor.commit()

        return self

    async def _export_table_to_s3(
        self, ctx: ExecutionContext, table_name: str, bounds: TableSyncBounds
    ):
        cursor = await ctx.aurora()

        blueprint = ctx.blueprint()
        table = blueprint.get_table(table_name)

        extract_main_query = EXTRACT_FROM_MAIN_TEMPLATE.format(
            table_cols=comma_separated_column_names(table.columns),
            main_table_name=source_table_name(table_name),
            lower_bound=bounds.next_extract_seq,
            upper_bound=bounds.max_extract_seq,
        )
        extract_shadow_query = EXTRACT_FROM_SHADOW_TEMPLATE.format(
            pkey_cols=comma_separated_column_names(table.primary_key),
            shadow_table_name=shadow_table_name(table_name),
            lower_bound=bounds.next_shadow_extract_seq,
            upper_bound=bounds.max_shadow_extract_seq,
        )
        extract_main = EXTRACT_S3_TEMPLATE.format(
            extract_query=extract_main_query,
            s3_bucket=ctx.s3_bucket(),
            s3_region=ctx.s3_region(),
            s3_file_path="{}{}".format(
                ctx.s3_path(),
                self._to_extract[table_name].writes_path().path_with_file(),
            ),
        )
        extract_shadow = EXTRACT_S3_TEMPLATE.format(
            extract_query=extract_shadow_query,
            s3_bucket=ctx.s3_bucket(),
            s3_region=ctx.s3_region(),
            s3_file_path="{}{}".format(
                ctx.s3_path(),
                self._to_extract[table_name].deletes_path().path_with_file(),
            ),
        )
        logger.debug("Running main export query: %s", extract_main)
        logger.debug("Running shadow export query: %s", extract_shadow)
        await cursor.execute(extract_main)
        await cursor.execute(extract_shadow)

    async def _complete_sync(
        self, ctx: ExecutionContext, table_name: str, bounds: TableSyncBounds
    ):
        cursor = await ctx.aurora()

        # NOTE: If any of the max extract sequence values are `MAX_SEQ`, it
        # indicates that there were no values to extract.
        if bounds.max_shadow_extract_seq != MAX_SEQ:
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


class ExtractLocation:
    def __init__(self, writes_path: S3Path, deletes_path: S3Path) -> None:
        # "Writes" instead of inserts because the deltas we extract from Aurora
        # contain inserts and updates.
        self._writes_path = writes_path
        self._deletes_path = deletes_path

    def writes_path(self) -> S3Path:
        return self._writes_path

    def deletes_path(self) -> S3Path:
        return self._deletes_path
