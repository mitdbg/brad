# See workloads/cross_db_benchmark/benchmark_tools/tidb/README.md

import argparse
import sys
from workloads.IMDB_extended.workload_utils.baseline import (
    PostgresCompatibleLoader,
    TiDBLoader,
    redshift_stress_test,
)
import time
import pickle
import numpy as np
import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", default="imdb")
    parser.add_argument("--dataset", default="imdb_extended")
    parser.add_argument("--load", default=False, action="store_true")
    parser.add_argument("--force-load", default=False, action="store_true")
    parser.add_argument("--load_from", default="")
    parser.add_argument("--run_query", default=None)
    parser.add_argument("--redshift-stress", default=False, action="store_true")
    parser.add_argument("--tidb-stress", default=False, action="store_true")
    parser.add_argument("--tidb-comparison", default=False, action="store_true")
    parser.add_argument("--engine", default="")
    parser.add_argument("--metrics", default=False, action="store_true")
    args = parser.parse_args()
    if args.redshift_stress:
        query_bank = "workloads/IMDB_100GB/ad_hoc/queries.sql"
        redshift_stress_test(query_bank, 64)
        sys.exit(0)
    if args.engine == "tidb":
        loader = TiDBLoader()
    elif args.engine == "aurora" or args.engine == "redshift":
        loader = PostgresCompatibleLoader(engine=args.engine)
    if args.load:
        assert args.engine != "tidb", "TIDB can only be loaded manually"
        loader.load_database(
            dataset=args.dataset,
            force=args.force_load,
            load_from=args.load_from,
        )
    if args.metrics:
        # From November 9th 2023 at 16:14 UTC to one hour later.
        from datetime import datetime, timedelta
        start_time = datetime(year=2023, month=12, day=3, hour=15, minute=12)
        end_time = start_time + timedelta(hours=1)
        df = loader.fetch_metrics(start_time=start_time, end_time=end_time)
        # Write to csv
        start_time = df['timestamp'].min()
        df['timestamp'] = (df['timestamp']-start_time).dt.total_seconds() // 60
        if args.engine == "aurora":
            df["cost"] = (0.16 * df["value"]) / 60 # $0.16 per hour.
        elif args.engine == "redshift":
            df["cost"] = (0.36 * df["value"]) / 60 # $0.36 per hour.
        df.to_csv(f"expt_out/metrics_{args.engine}.csv", index=False)
        print(f"Metrics:\n{df.head()}\n Mean Val: {df['value'].mean()}\n Mean Cost: {df['cost'].mean()}")
        sys.exit(0)
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
    if args.tidb_stress:
        query_bank = "adhoc_queries.sql"
        with open(query_bank, "r", encoding="utf-8") as f:
            queries = f.read().split(";")
        num_success = 0
        num_fail = 0
        fails = []
        for i, q in enumerate(queries):
            if i % 10 == 0:
                # Checkpoint to pkl
                print(f"Checkpoint {i}. Success: {num_success}, Fail: {num_fail}")
                pickle.dump(fails, open("fails.pkl", "wb"))
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
        # Write all to pkl.
        pickle.dump(fails, open("fails.pkl", "wb"))
    if args.tidb_comparison:
        numpy_file = "run_time_s-athena-aurora-redshift.npy"
        # Triplets of [athena, aurora, redshift] runtimes.
        run_times = np.load(numpy_file)
        df = pd.DataFrame(run_times, columns=["athena", "aurora", "redshift"])
        # Find indexes in which athena is faster than redshift and aurora.
        athena_faster = (df["athena"] < df["redshift"]) & (df["athena"] < df["aurora"])
        athena_faster = [i for i, x in enumerate(athena_faster) if x]
        print(f"Athena faster than redshift: {athena_faster}")
        print(f"Run time:\n{df.head()}\n{len(df)}")
        fails0 = pickle.load(open("fails0.pkl", "rb"))
        fails1 = pickle.load(open("fails1.pkl", "rb"))
        fails = set([i for i, _x in fails0])
        fails.update([i for i, _x in fails1])
        print(f"Fails: {len(fails)}. Fails0: {len(fails0)}, Fails1: {len(fails1)}")
        still_faster = [i for i in athena_faster if i not in fails]
        print(f"Still faster: {len(still_faster)}")
        query_bank = "adhoc_queries.sql"
        with open(query_bank, "r", encoding="utf-8") as f:
            queries = f.read().split(";")
            queries = [q.strip() for q in queries if len(q.strip()) > 0]
        res = [f"{q.strip()};" for i, q in enumerate(queries) if i not in fails]
        with open("good_adhoc_queries.sql", "w", encoding="utf-8") as f:
            f.write("\n".join(res))

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
