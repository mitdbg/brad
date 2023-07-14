import argparse
import multiprocessing as mp
import time
import sys
import signal
import threading
import os
import pathlib
from datetime import timedelta
from typing import List

from brad.calibration.query_runner import (
    run_until_signalled,
    get_run_specific_query,
    Options,
)
from brad.calibration.metrics import CLOUDWATCH_LOAD_METRICS
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.daemon.cloudwatch import CloudwatchClient
from brad.server.engine_connections import EngineConnections
from brad.connection.connection import Connection


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


def get_output_dir() -> None:
    # For printing out results.
    if "COND_OUT" in os.environ:
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(".")
    return out_dir


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", type=str, required=True)
    parser.add_argument("--schema-name", type=str, required=True)
    parser.add_argument(
        "--run_for_s",
        type=int,
        help="How long to run the experiment for. If unset, the experiment will run until Ctrl-C.",
    )
    parser.add_argument("--query_file", type=str, required=True)
    parser.add_argument("--specific_query_idx", type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run_warmup", action="store_true")
    # Controls how the clients submit queries to the underlying engine.
    parser.add_argument("--num_clients", type=int, default=1)
    parser.add_argument("--avg_gap_s", type=float, default=1.0)
    parser.add_argument("--std_gap_s", type=float, default=0.5)
    parser.add_argument("--wait_before_start", type=int)
    args = parser.parse_args()

    config = ConfigFile(args.config_file)
    queries = load_queries(args.query_file)

    if args.run_warmup:
        ec = EngineConnections.connect_sync(
            config,
            args.schema_name,
            autocommit=True,
            specific_engines={Engine.Redshift},
        )
        run_warmup(ec.get_connection(Engine.Redshift), queries)
        ec.close_sync()
        return

    assert args.specific_query_idx is not None
    assert args.specific_query_idx < len(queries)

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

    cw = CloudwatchClient(Engine.Redshift, config.redshift_cluster_id, config)
    qrunner = get_run_specific_query(
        args.specific_query_idx, queries[args.specific_query_idx]
    )

    processes = []
    for idx in range(args.num_clients):
        options = Options(
            idx,
            out_dir / f"redshift_runner_{idx}.csv",
            config,
            Engine.Redshift,
            args.schema_name,
        )
        options.disable_redshift_cache = True
        options.avg_gap_s = args.avg_gap_s
        options.std_gap_s = args.std_gap_s
        p = mp.Process(
            target=run_until_signalled, args=(qrunner, options, start_queue, stop_queue)
        )
        p.start()
        processes.append(p)

    print("Waiting for startup...", flush=True)
    for _ in range(args.num_clients):
        start_queue.get()

    print("Telling {} clients to start.".format(args.num_clients), flush=True)
    for _ in range(args.num_clients):
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
    for _ in range(args.num_clients):
        stop_queue.put("")

    print(
        "Waiting a few seconds before retrieving metrics...",
        flush=True,
        file=sys.stderr,
    )
    time.sleep(20)

    # Fetch Cloudwatch metrics for the duration of this workload.
    metrics = cw.fetch_metrics(
        CLOUDWATCH_LOAD_METRICS, period=timedelta(seconds=60), num_prev_points=10
    )
    metrics.to_csv(out_dir / "metrics.json", index=False)

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
