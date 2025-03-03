import asyncio
import argparse
import multiprocessing as mp
import time
import sys
import signal
import threading
import os
import pathlib
from datetime import timedelta
from typing import List, Optional

from brad.calibration.load.query_runner import (
    run_specific_query_until_signalled,
    Options,
)
from brad.calibration.load.metrics import (
    CLOUDWATCH_LOAD_METRICS,
    PERF_INSIGHTS_LOAD_METRICS,
)
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.daemon.cloudwatch import CloudWatchClient
from brad.daemon.perf_insights import PerfInsightsClient
from brad.connection.connection import Connection
from brad.connection.factory import ConnectionFactory
from brad.provisioning.directory import Directory
from brad.utils import set_up_logging


def load_queries(file_path: str) -> List[str]:
    with open(file_path, encoding="UTF-8") as file:
        return [line.strip() for line in file]


def run_warmup(connection: Connection, query_list: List[str]) -> None:
    cursor = connection.cursor_sync()
    for i, q in enumerate(query_list):
        start = time.time()
        cursor.execute_sync(q)
        _ = cursor.fetchall_sync()
        end = time.time()
        print(
            f"Warmed up {i} of {len(query_list)}. Took {(end - start):.2f} seconds.",
            file=sys.stderr,
            flush=True,
        )


def get_output_dir() -> pathlib.Path:
    # For printing out results.
    if "COND_OUT" in os.environ:
        import conductor.lib as cond  # pylint: disable=import-error

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(".")
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser(
        "Tool used to measure system load when queries are co-executed."
    )
    parser.add_argument(
        "--config-file-var",
        type=str,
        default="BRAD_CONFIG",
        help="Environment variable holding a path to BRAD's configuration file.",
    )
    parser.add_argument(
        "--schema-name-var",
        type=str,
        default="BRAD_SCHEMA",
        help="Environment variable holding the schema to run queries against.",
    )
    parser.add_argument(
        "--engine", type=str, required=True, help="The engine to run against."
    )
    parser.add_argument(
        "--run-for-s",
        type=int,
        help="How long to run the experiment for. If unset, the experiment will run until Ctrl-C.",
    )
    parser.add_argument(
        "--query-file",
        type=str,
        required=True,
        help="Path to a file containing queries to run.",
    )
    parser.add_argument(
        "--specific-query-idxs",
        type=str,
        help="Query indexes to run (by comma-separated index in the query file).",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for reproducibility."
    )
    parser.add_argument(
        "--run-warmup",
        action="store_true",
        help="If set, run the queries in the query file once to warm up the engine.",
    )
    parser.add_argument(
        "--wait-before-start",
        type=int,
        help="If set, wait this many seconds before starting to issue queries.",
    )
    parser.add_argument(
        "--avg-gap-s",
        type=float,
        default=1.0,
        help="Average amount of time to wait between issuing queries.",
    )
    parser.add_argument(
        "--std-gap-s",
        type=float,
        default=0.5,
        help="Std. dev. for the amount of time to wait between issuing queries.",
    )
    parser.add_argument("--debug", action="store_true", help="Set to enable debugging.")
    args = parser.parse_args()

    set_up_logging(debug_mode=args.debug)

    engine = Engine.from_str(args.engine)
    if engine == Engine.Athena:
        print("Athena is not supported.", file=sys.stderr, flush=True)
        return

    schema_name = os.environ[args.schema_name_var]
    config = ConfigFile.load(os.environ[args.config_file_var])
    queries = load_queries(args.query_file)

    directory = Directory(config)
    asyncio.run(directory.refresh())

    if args.run_warmup:
        conn = ConnectionFactory.connect_to_sync(engine, schema_name, config, directory)
        if engine == Engine.Redshift:
            cursor = conn.cursor_sync()
            cursor.execute_sync("SET enable_result_cache_for_session = off")
        run_warmup(conn, queries)
        conn.close_sync()
        return

    assert args.specific_query_idxs is not None
    qidxs = [int(idx.strip()) for idx in args.specific_query_idxs.split(",")]

    for qidx in qidxs:
        assert qidx < len(queries), f"Q{qidx} is out of bounds."

    if args.wait_before_start is not None:
        print(
            "Waiting {} seconds before starting...".format(args.wait_before_start),
            flush=True,
            file=sys.stderr,
        )
        time.sleep(args.wait_before_start)

    mgr = mp.Manager()
    start_queue = mgr.Queue()
    stop_queue = mgr.Queue()

    out_dir = get_output_dir()

    if engine == Engine.Redshift:
        cw: Optional[CloudWatchClient] = CloudWatchClient(
            Engine.Redshift,
            config.redshift_cluster_id,
            instance_identifier=None,
            config=config,
        )
        pi: Optional[PerfInsightsClient] = None
    else:
        cw = None
        aurora_instance_id = directory.aurora_writer().instance_id()
        print(
            "Using Aurora instance ID:", aurora_instance_id, file=sys.stderr, flush=True
        )
        pi = PerfInsightsClient.from_instance_identifier(aurora_instance_id, config)

    processes = []
    for client_num, query_index in enumerate(qidxs):
        options = Options(
            client_num,
            out_dir / f"runner_{client_num}.csv",
            config,
            engine,
            schema_name,
        )
        if engine == Engine.Redshift:
            options.disable_redshift_cache = True
        options.avg_gap_s = args.avg_gap_s
        options.std_gap_s = args.std_gap_s
        p = mp.Process(
            target=run_specific_query_until_signalled,
            args=(
                query_index,
                queries[query_index],
                options,
                start_queue,
                stop_queue,
            ),
        )
        p.start()
        processes.append(p)

    print("Waiting for startup...", flush=True)
    for _ in range(len(qidxs)):
        start_queue.get()

    print("Telling {} clients to start.".format(len(qidxs)), flush=True)
    for _ in range(len(qidxs)):
        stop_queue.put("")

    if args.run_for_s is not None:
        print(
            "Letting the experiment run for {} seconds...".format(args.run_for_s),
            flush=True,
            file=sys.stderr,
        )
        time.sleep(args.run_for_s)

    else:
        print(
            "Waiting until requested to stop... (hit Ctrl-C)",
            flush=True,
            file=sys.stderr,
        )
        should_shutdown = threading.Event()

        def signal_handler(_signal, _frame):
            should_shutdown.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        should_shutdown.wait()

    print("Stopping clients...", flush=True, file=sys.stderr)
    for _ in range(len(qidxs)):
        stop_queue.put("")

    print(
        "Waiting a few seconds before retrieving metrics...",
        flush=True,
        file=sys.stderr,
    )
    if cw is not None:
        time.sleep(30)
    elif pi is not None:
        time.sleep(20)

    if engine == Engine.Redshift:
        assert cw is not None
        # Fetch CloudWatch metrics for the duration of this workload.
        metrics = cw.fetch_metrics(
            CLOUDWATCH_LOAD_METRICS, period=timedelta(seconds=60), num_prev_points=30
        )
        metrics.to_csv(out_dir / "metrics.csv")
    elif engine == Engine.Aurora:
        assert pi is not None
        metrics_list = [(metric, "avg") for metric in PERF_INSIGHTS_LOAD_METRICS]
        metrics = pi.fetch_metrics(
            metrics_list, period=timedelta(seconds=60), num_prev_points=10
        )
        metrics.to_csv(out_dir / "metrics.csv")

    # Wait for the experiment to finish.
    for p in processes:
        p.join()

    print("Done!", flush=True, file=sys.stderr)


if __name__ == "__main__":
    # On Unix platforms, the default way to start a process is by forking, which
    # is not ideal (we do not want to duplicate this process' file
    # descriptors!).
    mp.set_start_method("spawn")
    main()
