import logging
from .operator import Operator

from brad.blueprint.sql_gen.table import comma_separated_column_names
from brad.config.dbtype import DBType
from brad.config.strings import delete_delta_table_name, insert_delta_table_name
from brad.data_sync.execution.context import ExecutionContext

logger = logging.getLogger(__name__)

_AURORA_REDSHIFT_DELETE_COMMAND = (
    "DELETE FROM {main_table} USING {delete_delta_table} WHERE {conditions}"
)
_AURORA_REDSHIFT_INSERT_COMMAND = (
    "INSERT INTO {main_table} SELECT * FROM {insert_delta_table}"
)

_ATHENA_MERGE_COMMAND = """
    MERGE INTO {main_table} AS t
    USING (
        WITH actual_deletes AS (
            SELECT {pkey_cols} FROM {delete_delta_table}
            EXCEPT
            SELECT {pkey_cols} FROM {insert_delta_table}
        )
        SELECT
            {pkey_cols},
            {other_cols},
            0 AS brad_is_delete
        FROM {insert_delta_table}
        UNION ALL
        SELECT
            {pkey_cols},
            {other_cols_as_null},
            1 AS brad_is_delete
        FROM actual_deletes
    ) AS s
    ON {merge_cond}
    WHEN MATCHED AND s.brad_is_delete = 1
        THEN DELETE
    WHEN MATCHED AND s.brad_is_delete != 1
        THEN UPDATE SET {update_cols}
    WHEN NOT MATCHED AND s.brad_is_delete != 1
        THEN INSERT VALUES ({insert_cols});
"""


class ApplyDeltas(Operator):
    def __init__(
        self, onto_table_name: str, from_table_name: str, engine: DBType
    ) -> None:
        super().__init__()
        self._onto_table_name = onto_table_name
        self._from_table_name = from_table_name
        self._engine = engine

    def __repr__(self) -> str:
        return "".join(
            [
                "ApplyDeltas(onto_table_name=",
                self._onto_table_name,
                ", from_table_name=",
                self._from_table_name,
                ", engine=",
                self._engine,
                ")",
            ]
        )

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        if self._engine == DBType.Aurora:
            aurora = await ctx.aurora()
            return await self._execute_aurora_redshift(ctx, aurora)
        elif self._engine == DBType.Redshift:
            redshift = await ctx.redshift()
            return await self._execute_aurora_redshift(ctx, redshift)
        elif self._engine == DBType.Athena:
            return await self._execute_athena(ctx)
        else:
            raise RuntimeError("Unsupported engine {}".format(self._engine))

    async def _execute_aurora_redshift(
        self, ctx: ExecutionContext, cursor
    ) -> "Operator":
        make_deletes = _AURORA_REDSHIFT_DELETE_COMMAND.format(
            main_table=self._onto_table_name,
            delete_delta_table=delete_delta_table_name(self._from_table_name),
            conditions=self._generate_aurora_redshift_delete_conditions(ctx),
        )
        logger.debug("Running on %s: %s", self._engine, make_deletes)
        await cursor.execute(make_deletes)

        make_inserts = _AURORA_REDSHIFT_INSERT_COMMAND.format(
            main_table=self._onto_table_name,
            insert_delta_table=insert_delta_table_name(self._from_table_name),
        )
        logger.debug("Running on %s: %s", self._engine, make_inserts)
        await cursor.execute(make_inserts)
        return self

    def _generate_aurora_redshift_delete_conditions(self, ctx: ExecutionContext) -> str:
        table = ctx.blueprint().get_table(self._onto_table_name)
        conditions = []
        for col in table.primary_key:
            conditions.append(
                "{main_table}.{col_name} = {delete_delta_table}.{col_name}".format(
                    main_table=self._onto_table_name,
                    delete_delta_table=delete_delta_table_name(
                        self._from_table_name,
                    ),
                    col_name=col.name,
                )
            )
        return " AND ".join(conditions)

    async def _execute_athena(self, ctx: ExecutionContext) -> "Operator":
        table = ctx.blueprint().get_table(self._onto_table_name)
        pkey_cols = comma_separated_column_names(table.primary_key)
        non_primary_cols = list(filter(lambda c: not c.is_primary, table.columns))
        other_cols = comma_separated_column_names(non_primary_cols)
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

        query = _ATHENA_MERGE_COMMAND.format(
            main_table=self._onto_table_name,
            pkey_cols=pkey_cols,
            insert_delta_table=insert_delta_table_name(self._from_table_name),
            delete_delta_table=delete_delta_table_name(self._from_table_name),
            other_cols=other_cols,
            other_cols_as_null=other_cols_as_null,
            merge_cond=merge_cond,
            update_cols=update_cols,
            insert_cols=insert_cols,
        )
        athena = await ctx.athena()
        logger.debug("Running on Athena: %s", query)
        await athena.execute(query)
        return self
