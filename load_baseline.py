# See workloads/cross_db_benchmark/benchmark_tools/tidb/README.md

import argparse
import sys
from workloads.IMDB_extended.workload_utils.baseline import (
    PostgresCompatibleLoader,
    TiDBLoader,
)
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", default="imdb")
    parser.add_argument("--dataset", default="imdb_extended")
    parser.add_argument("--force_load", default=False, action="store_true")
    parser.add_argument("--load_from", default="")
    parser.add_argument("--run_query", default=None)
    parser.add_argument("--run_all", default=False, action="store_true")
    parser.add_argument("--engine", default="tidb")
    args = parser.parse_args()
    if args.engine == "tidb":
        # TIDB loaded manually
        loader = TiDBLoader()
    else:
        loader = PostgresCompatibleLoader(engine=args.engine)
        loader.load_database(
            dataset=args.dataset,
            force=args.force_load,
            load_from=args.load_from,
        )
    if args.run_query is not None:
        cur = loader.conn.cursor()
        print(f"Executing: {args.run_query}")
        start_time = time.perf_counter()
        cur.execute(args.run_query)
        res = cur.fetchall()
        end_time = time.perf_counter()
        print(f"Result length: {len(res)}")
        for r in res:
            print(r)
        print(f"Execution took: {end_time-start_time}s")
        loader.conn.commit()
    if args.run_all:
        query_bank = "workloads/IMDB_100GB/ad_hoc/queries.sql"
        with open(query_bank, "r", encoding="utf-8") as f:
            queries = f.read().split(";")
        num_success = 0
        num_fail = 0
        fails = []
        # Select 100 queries at random
        import random
        random.shuffle(queries)
        queries = queries[:100]
        for i, q in enumerate(queries):
            try:
                cur = loader.conn.cursor()
                print(f"Executing: {q}")
                start_time = time.perf_counter()
                cur.execute(q)
                res = cur.fetchall()
                end_time = time.perf_counter()
                print(f"Result length: {len(res)}")
                for r in res:
                    print(r)
                print(f"Execution took: {end_time-start_time}s")
                loader.conn.commit()
                num_success += 1
            except Exception as e:
                print(f"Error: {e}")
                loader.conn.rollback()
                num_fail += 1
                fails.append((i, f"Error: {e}"))
        print(f"Success: {num_success}, Fail: {num_fail}")
        print(f"Fails: \n{fails}")

if __name__ == "__main__":
    main()
    sys.exit(0)

import yaml


def column_definition(column):
    data_type = column["data_type"].upper()
    if data_type == "VARCHAR" or data_type == "CHARACTER VARYING":
        # Arbitrary length string. Write as TEXT for compatibility
        data_type = "TEXT"
    if data_type.startswith("CHARACTER VAR"):
        data_type = "TEXT"
    sql = f"{column['name']} {data_type}"
    if "primary_key" in column and column["primary_key"]:
        sql += " PRIMARY KEY"
    return sql


def table_definition(table):
    columns_sql = ",\n    ".join(column_definition(col) for col in table["columns"])
    sql = f"CREATE TABLE {table['table_name']} (\n    {columns_sql}\n);"
    return sql


def index_definition(table_name, index_columns):
    index_name = f"{table_name}_{'_'.join(index_columns)}_idx"
    print(type(index_columns))
    columns_str = ", ".join(index_columns)
    return f"CREATE INDEX {index_name} ON {table_name} ({columns_str});"


def yaml_main():
    with open("config/schemas/imdb_extended.yml", "r", encoding="utf-8") as f:
        tables = yaml.safe_load(f)
        print(f"Tables: {tables}")

    with open("tables.sql", "w", encoding="utf-8") as f:
        for table in tables["tables"]:
            # Table Definition
            f.write(f"DROP TABLE IF EXISTS {table['table_name']};\n")
            f.write(table_definition(table))
            f.write("\n\n")

            # Index Definitions
            if "indexes" in table:
                for index in table["indexes"]:
                    if isinstance(index, str):
                        index = index.split(",")
                        index = [n.strip() for n in index]
                    f.write(index_definition(table["table_name"], index))
                    f.write("\n")
                f.write("\n")


if __name__ == "__main__":
    yaml_main()
    sys.exit(0)
