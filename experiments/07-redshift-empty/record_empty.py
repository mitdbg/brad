import argparse
import datetime
import os
import pyodbc
import pathlib
import time

from typing import List


def load_queries(file_path: str) -> List[str]:
    queries = []
    with open(file_path, "r") as file:
        for line in file:
            query = line.strip()
            if query:
                queries.append(query)
    return queries


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cstr_var", type=str, required=True, help="The ODBC connection string"
    )
    parser.add_argument(
        "--query_file",
        type=str,
        default="../../workloads/IMDB/OLAP_queries/all_queries.sql",
    )
    parser.add_argument("--start_offset", type=int, default=0)
    parser.add_argument("--out_dir", type=str, default=".")
    args = parser.parse_args()

    curr_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_file_name = f"empty_queries_{curr_time}_offset_{args.start_offset}.csv"
    out_path = pathlib.Path(args.out_dir) / out_file_name

    conn = pyodbc.connect(os.environ[args.cstr_var])
    cursor = conn.cursor()
    cursor.execute("SET enable_result_cache_for_session = OFF;")

    queries = load_queries(args.query_file)
    total = len(queries)

    with open(out_path, "w") as out_file:
        print("query_idx,empty_run_time_s", file=out_file, flush=True)

        for idx, q in enumerate(queries):
            if idx < args.start_offset:
                continue

            if idx % 100 == 0:
                print("Running index {} of {}...".format(idx, total))

            start = time.time()
            cursor.execute(q)
            cursor.fetchall()
            end = time.time()
            rt = end - start

            print("{},{}".format(idx, rt), file=out_file, flush=True)


if __name__ == "__main__":
    main()
