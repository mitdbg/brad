import argparse
import boto3
import json
import time
import logging
import shutil
import pathlib
import datetime
import pyodbc
import os
import numpy as np

from typing import Any, Dict, List
from tqdm import tqdm

from brad.utils import set_up_logging

logger = logging.getLogger(__name__)


def extract_stats(cursor) -> Dict[str, List[List[Any]]]:
    data = cursor.fetchall()
    cols = [column[0] for column in cursor.description]

    return {
        "cols": cols,
        "data": [list(row) for row in data],
    }


def run_query(args, cursor, query: str, qidx: int) -> Dict[str, Any]:
    try:
        # Clear recorded statistics.
        cursor.execute("SELECT pg_stat_reset()")

        # Execute the query (indirectly, using EXPLAIN ANALYZE).
        start = time.time()
        cursor.execute(f"EXPLAIN ANALYZE {query}")
        raw_exec_plan = cursor.fetchall()
        exec_plan = [row[0] for row in raw_exec_plan]
        end = time.time()

        # Extract the statistics we need.
        cursor.execute("SELECT * FROM pg_stat_user_tables;")
        logical = extract_stats(cursor)

        cursor.execute("SELECT * FROM pg_statio_user_tables;")
        physical = extract_stats(cursor)

        cursor.execute("SELECT * FROM pg_stat_user_indexes;")
        index = extract_stats(cursor)

        return {
            "query_index": qidx,
            "status": "SUCCEEDED",
            "exec_plan": exec_plan,
            "run_time_s": end - start,
            "logical": logical,
            "physical": physical,
            "index": index,
        }

    except pyodbc.Error:
        logger.exception("Query index %d probably timed out.", qidx)

        return {
            "query_index": qidx,
            "status": "TIMEOUT",
            "run_time_s": args.timeout_s,
        }


def load_all_queries(queries_file: str) -> List[str]:
    with open(queries_file, "r", encoding="UTF-8") as file:
        return [line.strip() for line in file]


def save_checkpoint(data: List[Dict[str, Any]], output_file: pathlib.Path):
    temp_path = output_file.with_name(output_file.name + "_temp")
    with open(temp_path, "w", encoding="UTF-8") as outfile:
        json.dump(data, outfile, indent=2, default=str)
    shutil.move(temp_path, output_file)


def get_output_file_name(args) -> pathlib.Path:
    curr_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_file_name = (
        f"aurora+{args.prefix}+{curr_time}+{args.rank}+{args.world_size}.json"
    )
    return pathlib.Path(out_file_name)


def main():
    parser = argparse.ArgumentParser(
        "Tool used to gather training data on Aurora, using pyodbc."
    )
    parser.add_argument(
        "--prefix",
        type=str,
        required=True,
        help="Used to identify a data collection run.",
    )
    parser.add_argument(
        "--queries-file",
        type=str,
        required=True,
        help="Path to a file containing SQL queries, one per line.",
    )
    parser.add_argument(
        "--cluster-id",
        type=str,
        required=True,
        help="The Aurora cluster ID to connect to.",
    )
    parser.add_argument(
        "--pwdvar",
        type=str,
        required=True,
        help="The name of the environment variable that stores the cluster password.",
    )
    parser.add_argument("--port", type=int, default=5432, help="The cluster's port.")
    parser.add_argument("--schema-name", type=str, default="imdb")
    parser.add_argument(
        "--timeout-s", type=float, default=200.0, help="The query timeout in seconds."
    )
    parser.add_argument(
        "--existing-run-times",
        type=str,
        help="Path to an existing measured query run time file, if it exists.",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=50,
        help="Checkpoint the gathered data every X queries.",
    )
    parser.add_argument(
        "--resume-from-index",
        type=int,
        help="If set, resume collection from the specified index.",
    )
    # Used for parallelizing the data collection.
    parser.add_argument("--world-size", type=int, default=1)
    parser.add_argument("--rank", type=int, default=0)
    args = parser.parse_args()

    set_up_logging()

    if args.rank >= args.world_size:
        raise RuntimeError("Rank must be less than the world size.")

    # Compute start/end offsets.
    queries = load_all_queries(args.queries_file)
    queries_per_worker = len(queries) // args.world_size

    query_start_offset = queries_per_worker * args.rank
    query_end_offset = queries_per_worker * (args.rank + 1)  # Exclusive.
    if args.rank == args.world_size - 1:
        # For simplicity, the last worker takes on the remainder queries.
        query_end_offset = len(queries)

    if args.existing_run_times is not None:
        existing_run_times = np.load(args.existing_run_times)
    else:
        existing_run_times = None

    logger.info("Rank %d, World size: %d.", args.rank, args.world_size)
    logger.info(
        "Start offset (inclusive): %d, End offset (exclusive): %d",
        query_start_offset,
        query_end_offset,
    )

    if args.resume_from_index is not None:
        query_start_offset = args.resume_from_index
        logger.info(
            "Actually starting from index %d",
            query_start_offset,
        )

    client = boto3.client("rds")

    # Retrieve the cluster endpoint.
    response = client.describe_db_clusters(DBClusterIdentifier=args.cluster_id)
    endpoint = response["DBClusters"][0]["Endpoint"]
    logger.info("Resolved cluster endpoint: %s", endpoint)

    # Connect to the database.
    cstr = "Driver={{{}}};Server={};Port={};Uid={};Pwd={};".format(
        "Postgres",
        endpoint,
        args.port,
        "postgres",
        os.environ[args.pwdvar],
    )
    if args.schema_name is not None:
        cstr += "Database={};".format(args.schema_name)

    connection = pyodbc.connect(cstr, autocommit=True)
    connection.execute(
        f"ALTER DATABASE {args.schema_name} SET statement_timeout = '{int(args.timeout_s)}s';"
    )
    connection.commit()
    connection.close()

    connection = pyodbc.connect(cstr, autocommit=True)
    cursor = connection.cursor()

    recorded_results = []
    num_recorded = 0

    output_file_path = get_output_file_name(args)
    logger.info("Will save results in %s", output_file_path)

    overall_start = time.time()
    for query_idx, query_str in enumerate(tqdm(queries)):
        if query_idx < query_start_offset:
            continue
        if query_idx >= query_end_offset:
            break

        if existing_run_times is not None and (
            np.isinf(existing_run_times[query_idx])
            or np.isnan(existing_run_times[query_idx])
        ):
            logger.info(
                "Skipping query index %d because it will cause a timeout.", query_idx
            )
            recorded_results.append(
                {
                    "query_index": query_idx,
                    "status": "TIMEOUT",
                    "run_time_s": args.timeout_s,
                }
            )

        else:
            # Make sure the connection is still alive.
            connection_retry_count = 0
            while True:
                try:
                    cursor.execute("SELECT 1")
                    _ = cursor.fetchall()
                    logger.info("Reconnected to Aurora.")
                    break
                except pyodbc.Error as ex:
                    if connection_retry_count > 10:
                        raise RuntimeError("Connection failed.") from ex
                connection_retry_count += 1
                connection = pyodbc.connect(cstr, autocommit=True)
                cursor = connection.cursor()

            try:
                recorded_results.append(run_query(args, cursor, query_str, query_idx))
            except Exception as ex:
                if isinstance(ex, KeyboardInterrupt):
                    # User initiated abort (via Ctrl-C).
                    logger.info("Aborting...")
                    break

                # Log errors, but do not abort the data collection.
                logger.exception("Encountered error while running a query.")

        num_recorded += 1

        if num_recorded % args.checkpoint_every == 0:
            save_checkpoint(recorded_results, output_file_path)
            logger.info(
                "Completed index %d. Will stop at index %d.",
                query_idx,
                query_end_offset,
            )

    save_checkpoint(recorded_results, output_file_path)
    overall_end = time.time()
    logger.info("Done! Took %.2f seconds in total.", overall_end - overall_start)


if __name__ == "__main__":
    main()
