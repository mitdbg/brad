import csv
import logging
from brad.blueprint import Blueprint
from brad.blueprint.sql_gen.table import TableSqlGenerator
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.connection.connection import Connection

logger = logging.getLogger(__name__)


def create_tables_in_stub(
    config: ConfigFile, connection: Connection, blueprint: Blueprint
) -> None:
    cursor = connection.cursor_sync()
    sqlgen = TableSqlGenerator(config, blueprint)
    for table in blueprint.tables():
        try:
            # We check if the table exists.
            cursor.execute_sync(f"SELECT 1 FROM {table.name}")
            continue
        except:  # pylint: disable=bare-except
            pass
        logger.info("Creating stub table %s", table.name)
        queries, _ = sqlgen.generate_create_table_sql(table, location=Engine.Redshift)
        for q in queries:
            # HACK: To support SQLite DDL syntax.
            if "VARCHAR(MAX)" in q:
                q = q.replace("VARCHAR(MAX)", "VARCHAR(65535)")
            cursor.execute_sync(q)


def load_tables_in_stub(
    config: ConfigFile, connection: Connection, blueprint: Blueprint
) -> None:
    stub_path = config.stub_mode_path()
    if stub_path is None:
        return

    cursor = connection.cursor_sync()
    cursor.execute_sync("BEGIN")

    for table in blueprint.tables():
        cursor.execute_sync(f"SELECT COUNT(*) FROM {table.name}")
        rows = cursor.fetchall_sync()
        if len(rows) > 0 and int(rows[0][0]) > 0:
            # The table is non-empty.
            continue

        # Load the raw data.
        logger.info("Loading stub data into %s", table.name)
        with open(
            stub_path / "dataset" / f"{table.name}.csv", "r", encoding="UTF-8"
        ) as file:
            reader = csv.reader(file, delimiter="|")
            ncols = len(table.columns)
            placeholders = ", ".join(["?"] * ncols)
            raw_query = f"INSERT INTO {table.name} VALUES ({placeholders})"
            for idx, row in enumerate(reader):
                if idx == 0:
                    continue
                # Ideally we should insert in batches. But our stub datasets are
                # very small (~100 rows), so we keep the implementation simple.
                cursor.executemany_sync(raw_query, [row])

    cursor.execute_sync("COMMIT")
