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
        queries, _ = sqlgen.generate_create_table_sql(table, location=Engine.Redshift)
        for q in queries:
            logger.info(table.name)
            logger.info(q)
            # HACK: To support SQLite DDL syntax.
            if "VARCHAR(MAX)" in q:
                q = q.replace("VARCHAR(MAX)", "VARCHAR(65535)")
            cursor.execute_sync(q)
