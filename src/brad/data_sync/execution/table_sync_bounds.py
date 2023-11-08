import logging
from typing import List, Dict

from brad.config.strings import (
    AURORA_EXTRACT_PROGRESS_TABLE_NAME,
    AURORA_SEQ_COLUMN,
    shadow_table_name,
    source_table_name,
)
from brad.data_sync.execution.context import ExecutionContext

logger = logging.getLogger(__name__)

# Represents the largest possible sequence value (BIGSERIAL).
# Ideally we should be using the DBMS' value.
MAX_SEQ = 0xFFFFFFFF_FFFFFFFF

# Used to get the state of data synchronization (i.e., where did we "stop" last time the sync job ran?)
GET_NEXT_EXTRACT_TEMPLATE = (
    "SELECT table_name, next_extract_seq, next_shadow_extract_seq FROM "
    + AURORA_EXTRACT_PROGRESS_TABLE_NAME
    + " WHERE table_name IN ({extract_tables})"
)
GET_MAX_EXTRACT_TEMPLATE = "SELECT MAX(" + AURORA_SEQ_COLUMN + ") FROM {table_name}"


class TableSyncBounds:
    """
    Used to determine whether or not a table has changes that need to be synced.
    """

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
            # MAX_SEQ means that the table(s) are empty.
            (
                self.next_extract_seq > self.max_extract_seq
                or self.max_extract_seq == MAX_SEQ
            )
            and (
                self.next_shadow_extract_seq > self.max_shadow_extract_seq
                or self.max_shadow_extract_seq == MAX_SEQ
            )
        )

    def should_advance_main_seq(self) -> bool:
        """
        Returns true when the extraction range is non-empty (and so the next
        extraction should start at a higher sequence number).
        """
        return (
            self.max_extract_seq != MAX_SEQ
            and self.max_extract_seq >= self.next_extract_seq
        )

    def should_advance_shadow_seq(self) -> bool:
        """
        Returns true when the shadow table extraction range is non-empty (and so the next
        extraction should start at a higher sequence number).
        """
        return (
            self.max_shadow_extract_seq != MAX_SEQ
            and self.max_shadow_extract_seq >= self.next_shadow_extract_seq
        )

    def __repr__(self) -> str:
        return "TableSyncBounds(main_seq_range=[{}, {}], shadow_seq_range=[{}, {}])".format(
            self.next_extract_seq,
            self.max_extract_seq,
            self.next_shadow_extract_seq,
            self.max_shadow_extract_seq,
        )

    @staticmethod
    async def get_table_sync_bounds_for(
        tables: List[str],
        ctx: ExecutionContext,
    ) -> Dict[str, "TableSyncBounds"]:
        cursor = await ctx.aurora()

        if len(tables) == 0:
            return {}

        # 1. Retrieve the starting sequence values for extraction.
        q = GET_NEXT_EXTRACT_TEMPLATE.format(
            extract_tables=", ".join(map("'{}'".format, tables))
        )
        logger.debug("Executing on Aurora %s", q)
        await cursor.execute(q)

        # NOTE: A lower bound for the shadow table is not absolutely needed
        # because we run under strict serializable isolation (to ensure we
        # always extract a transactionally-consistent snapshot).

        # table_name: (main_bound, shadow_bound)
        table_bounds: Dict[str, TableSyncBounds] = {}
        async for row in cursor:
            bounds = TableSyncBounds()
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
                bounds.max_extract_seq = MAX_SEQ
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
                bounds.max_shadow_extract_seq = MAX_SEQ
            else:
                bounds.max_shadow_extract_seq = row[0]

        return table_bounds
