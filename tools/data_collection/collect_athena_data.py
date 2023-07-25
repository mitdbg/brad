import argparse
import boto3
import json
import time
import logging
import shutil
import pathlib
import datetime
import numpy as np

from typing import Any, Dict, List

from brad.config.file import ConfigFile
from brad.utils import set_up_logging

logger = logging.getLogger(__name__)


def collect_runtime_statistics(
    client, query_exec_id: str, max_attempts: int
) -> Dict[str, Any]:
    attempts = 0

    while True:
        response = client.get_query_runtime_statistics(QueryExecutionId=query_exec_id)
        runtime_stats = response["QueryRuntimeStatistics"]
        attempts += 1

        if "Rows" in runtime_stats:
            return runtime_stats

        if attempts >= max_attempts:
            break

        # Wait before checking the status again. Sometimes the `Rows` metadata
        # is delayed from being propagated.
        time.sleep(0.5)

    # Default value.
    return {}


def run_query(
    args, client, query: str, qidx: int, config: ConfigFile
) -> Dict[str, Any]:
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": args.schema_name},
        ResultConfiguration={"OutputLocation": config.athena_s3_output_path},
    )

    # Get query execution ID
    query_execution_id = response["QueryExecutionId"]

    # Get query execution details
    query_execution = client.get_query_execution(QueryExecutionId=query_execution_id)

    # Set start time for timeout
    start_time = time.time()

    # Wait for query execution to finish or timeout
    while True:
        status = query_execution["QueryExecution"]["Status"]["State"]

        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        # Check timeout
        elapsed_time = time.time() - start_time
        if elapsed_time > args.timeout_s:
            # Cancel the query execution if timeout exceeded
            client.stop_query_execution(QueryExecutionId=query_execution_id)
            logger.warning("Timeout exceeded. Query index %d cancelled.", qidx)
            status = "TIMEOUT"
            break

        # Wait for 1 second before checking the status again
        time.sleep(1)

        # Get updated query execution details
        query_execution = client.get_query_execution(
            QueryExecutionId=query_execution_id
        )

    # Get query execution statistics if the query finished successfully
    exec_info = query_execution["QueryExecution"]

    if status == "SUCCEEDED":
        runtime_stats = collect_runtime_statistics(
            client, query_execution_id, max_attempts=5
        )
        return {
            "query_index": qidx,
            "status": status,
            "exec_info": exec_info,
            "runtime_stats": runtime_stats,
        }
    else:
        return {
            "query_index": qidx,
            "status": status,
            "exec_info": exec_info,
            "runtime_stats": {},
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
        f"athena+{args.prefix}+{curr_time}+{args.rank}+{args.world_size}.json"
    )
    return pathlib.Path(out_file_name)


def main():
    parser = argparse.ArgumentParser(
        "Tool used to gather training data on Athena, using Athena-specific APIs"
    )
    parser.add_argument(
        "--prefix",
        type=str,
        required=True,
        help="Used to identify a data collection run.",
    )
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to the BRAD configuration file.",
    )
    parser.add_argument(
        "--queries-file",
        type=str,
        required=True,
        help="Path to a file containing SQL queries, one per line.",
    )
    parser.add_argument("--schema-name", type=str, default="imdb")
    parser.add_argument(
        "--timeout-s", type=float, default=200.0, help="The query timeout in seconds."
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=50,
        help="Checkpoint the gathered data every X queries.",
    )
    parser.add_argument(
        "--existing-run-times",
        type=str,
        help="Path to existing recorded Athena run times (to skip timed out queries).",
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

    if args.existing_run_times is not None:
        existing_run_times = np.load(args.existing_run_times)
    else:
        existing_run_times = None

    # Compute start/end offsets.
    queries = load_all_queries(args.queries_file)
    queries_per_worker = len(queries) // args.world_size

    query_start_offset = queries_per_worker * args.rank
    query_end_offset = queries_per_worker * (args.rank + 1)  # Exclusive.
    if args.rank == args.world_size - 1:
        # For simplicity, the last worker takes on the remainder queries.
        query_end_offset = len(queries)

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

    config = ConfigFile(args.config_file)
    client = boto3.client(
        "athena",
        aws_access_key_id=config.aws_access_key,
        aws_secret_access_key=config.aws_access_key_secret,
    )

    recorded_results = []
    num_recorded = 0

    output_file_path = get_output_file_name(args)
    logger.info("Will save results in %s", output_file_path)

    overall_start = time.time()
    for query_idx, query_str in enumerate(queries):
        if query_idx < query_start_offset:
            continue
        if query_idx >= query_end_offset:
            break

        if existing_run_times is not None and np.isinf(existing_run_times[query_idx]):
            logger.info(
                "Skipping query index %d because it will cause a timeout.", query_idx
            )
            recorded_results.append(
                {
                    "query_index": query_idx,
                    "status": "TIMEOUT",
                    "exec_info": {},
                    "runtime_stats": {},
                }
            )
            continue

        try:
            recorded_results.append(
                run_query(args, client, query_str, query_idx, config)
            )
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
