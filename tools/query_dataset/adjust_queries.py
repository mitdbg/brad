import argparse
import asyncio
import yaml
from typing import Dict, List, Tuple

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.connection.connection import Connection
from brad.connection.factory import ConnectionFactory
from brad.provisioning.directory import Directory


def load_indexed_cols(schema_file: str) -> Dict[str, List[str]]:
    with open(schema_file) as file:
        raw_schema = yaml.load(file, yaml.Loader)

    # Retrieve all tables indexed columns in the schema
    # Note that we only index numeric columns, so this is OK for this script.
    table_indexed_cols = {}
    for table in raw_schema["tables"]:
        name = table["table_name"]
        indexed = []

        for column in table["columns"]:
            if "primary_key" in column and column["primary_key"]:
                indexed.append(column["name"])

        if "indexes" not in table:
            continue

        for index in table["indexes"]:
            parts = index.split(",")
            if len(parts) > 1:
                # Skip composite indexes.
                continue
            indexed.append(parts[0])

        table_indexed_cols[name] = indexed
    return table_indexed_cols


def load_indexed_column_stats(
    connection: Connection, table_indexed_cols: Dict[str, List[str]]
) -> Dict[str, Dict[str, Tuple[int, int]]]:
    cursor = connection.cursor_sync()
    table_info = {}

    for table_name, indexed_cols in table_indexed_cols.items():
        column_info = {}
        for column in indexed_cols:
            cursor.execute_sync(
                f"SELECT MIN({column}), MAX({column}) FROM {table_name};"
            )
            min_value, max_value = cursor.fetchone_sync()
            if min_value is None or max_value is None:
                print("NOTE: Column has all NULLs: {}.{}".format(table_name, column))
                continue
            column_info[column] = (min_value, max_value)

        table_info[table_name] = column_info

    return table_info


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", type=str, required=True)
    parser.add_argument("--schema-name", type=str, default="imdb_extended_20g")
    parser.add_argument("--schema-file", type=str, required=True)
    args = parser.parse_args()

    table_indexed_cols = load_indexed_cols(args.schema_file)

    config = ConfigFile(args.config_file)
    directory = Directory(config)
    asyncio.run(directory.refresh())
    conn = ConnectionFactory.connect_to_sync(
        Engine.Aurora, args.schema_name, config, directory
    )
    indexed_col_stats = load_indexed_column_stats(conn, table_indexed_cols)
    conn.close_sync()

    for table, info in indexed_col_stats.items():
        print(f"Table: {table}")
        for column, data in info.items():
            print(f"  Column: {column}, Min: {data[0]}, Max: {data[1]}")
        print()


if __name__ == "__main__":
    main()
