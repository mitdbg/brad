import argparse
import multiprocessing as mp
import pyodbc
import queue
import random
import time
import os
import pathlib
import signal
import threading

from typing import List
from datetime import timedelta
from metrics import MetricsHelper

METRICS = [
    "redshift_CPUUtilization_Average",
    "redshift_CPUUtilization_Minimum",
    "redshift_CPUUtilization_Maximum",
    "redshift_ReadIOPS_Average",
    "redshift_ReadIOPS_Minimum",
    "redshift_ReadIOPS_Maximum",
    "aurora_WRITER_CPUUtilization_Average",
    "aurora_WRITER_CPUUtilization_Minimum",
    "aurora_WRITER_CPUUtilization_Maximum",
    "aurora_WRITER_ReadIOPS_Average",
    "aurora_WRITER_ReadIOPS_Minimum",
    "aurora_WRITER_ReadIOPS_Maximum",
    "aurora_WRITER_DatabaseConnections_Minimum",
    "aurora_WRITER_DatabaseConnections_Maximum",
    "aurora_WRITER_DatabaseConnections_Average",
]


def load_queries(file_path: str) -> List[str]:
    queries = []
    with open(file_path, "r") as file:
        for line in file:
            query = line.strip()
            if query:
                queries.append(query)
    return queries


def runner(idx: int, start_queue: mp.Queue, stop_queue: mp.Queue, args):
    cstr = os.environ[args.cstr_var]
    conn = pyodbc.connect(cstr, autocommit=True)
    cursor = conn.cursor()

    # Hacky way to disable the query cache when applicable.
    if "Redshift" in cstr or "redshift" in cstr:
        print("Disabling Redshift result cache (client {})".format(idx))
        cursor.execute("SET enable_result_cache_for_session = OFF;")

    queries = load_queries(args.query_file)
    prng = random.Random(args.seed ^ idx)

    # For printing out results.
    if "COND_OUT" in os.environ:
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(".")

    with open(out_dir / "olap_batch_{}.csv".format(idx), "w") as file:
        print("query_idx,run_time_s", file=file, flush=True)

        # Signal that we're ready to start and wait for the controller.
        start_queue.put_nowait("")
        _ = stop_queue.get()

        if args.run_all_times is None:
            while True:
                wait_for_s = prng.gauss(args.avg_gap_s, args.std_gap_s)
                if wait_for_s < 0.0:
                    wait_for_s = 0.0
                time.sleep(wait_for_s)

                if args.specific_query_idx is None:
                    next_query_idx = prng.randrange(len(queries))
                else:
                    next_query_idx = args.specific_query_idx
                next_query = queries[next_query_idx]

                start = time.time()
                cursor.execute(next_query)
                cursor.fetchall()
                end = time.time()
                print(
                    "{},{}".format(next_query_idx, end - start), file=file, flush=True
                )

                try:
                    _ = stop_queue.get_nowait()
                    break
                except queue.Empty:
                    pass
        else:
            for idx, q in enumerate(queries):
                if idx % 10 == 0:
                    print(
                        "Running query index {} of {}".format(idx, len(queries)),
                        flush=True,
                    )
                for _ in range(args.run_all_times):
                    start = time.time()
                    cursor.execute(q)
                    cursor.fetchall()
                    end = time.time()
                    print("{},{}".format(idx, end - start), file=file)


def run_warmup(args):
    cstr = os.environ[args.cstr_var]
    conn = pyodbc.connect(cstr)
    try:
        conn.timeout = 30
    except pyodbc.Error:
        pass
    cursor = conn.cursor()

    # Hacky way to disable the query cache when applicable.
    if "Redshift" in cstr or "redshift" in cstr:
        print("Disabling Redshift result cache")
        cursor.execute("SET enable_result_cache_for_session = OFF;")

    queries = load_queries(args.query_file)
    with open("olap_batch_warmup.csv", "w") as file:
        print("query_idx,run_time_s", file=file)
        for idx, q in enumerate(queries):
            start = time.time()
            cursor.execute(q)
            cursor.fetchall()
            end = time.time()
            run_time_s = end - start
            print(
                "Warmed up {} of {}. Run time (s): {}".format(
                    idx + 1, len(queries), run_time_s
                )
            )
            if run_time_s >= 29:
                print("Warning: Query index {} takes longer than 30 s".format(idx))
            print("{},{}".format(idx, run_time_s), file=file, flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cstr_var", type=str, required=True, help="The ODBC connection string"
    )
    parser.add_argument(
        "--run_for_s",
        type=int,
        help="How long to run the experiment for. If unset, the experiment will run until Ctrl-C.",
    )
    parser.add_argument(
        "--run_all_times",
        type=int,
        help="If set, run all the queries in the file this many times.",
    )
    parser.add_argument("--query_file", type=str, default="queries.sql")
    parser.add_argument("--specific_query_idx", type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run_warmup", action="store_true")
    # Controls how the clients submit queries to the underlying engine.
    parser.add_argument("--num_clients", type=int, default=1)
    parser.add_argument("--avg_gap_s", type=float, default=1.0)
    parser.add_argument("--std_gap_s", type=float, default=0.5)
    parser.add_argument("--aurora_cluster", type=str, default="aurora-2")
    parser.add_argument("--redshift_cluster", type=str, default="redshift-ra3-test")
    args = parser.parse_args()

    if args.run_warmup:
        run_warmup(args)
        return

    mgr = mp.Manager()
    start_queue = mgr.Queue()
    stop_queue = mgr.Queue()

    metrics_reader = MetricsHelper(
        aurora_cluster_name=args.aurora_cluster,
        redshift_cluster_name=args.redshift_cluster,
        epoch_length=timedelta(minutes=3),
    )

    processes = []
    for idx in range(args.num_clients):
        p = mp.Process(target=runner, args=(idx, start_queue, stop_queue, args))
        p.start()
        processes.append(p)

    print("Waiting for startup...", flush=True)
    for _ in range(args.num_clients):
        start_queue.get()

    print("Telling {} clients to start.".format(args.num_clients), flush=True)
    for _ in range(args.num_clients):
        stop_queue.put("")

    if args.run_all_times is None:
        if args.run_for_s is not None:
            print(
                "Letting the experiment run for {} seconds...".format(args.run_for_s),
                flush=True,
            )
            current_time = time.time()
            stop_time = current_time + args.run_for_s
            while True:
                metrics_reader.load_metrics()
                now = time.time()
                if now < stop_time:
                    time.sleep(10)
                else:
                    print(metrics_reader.get_metrics(metric_ids=METRICS), flush=True)
                    break

        else:
            print("Waiting until requested to stop... (hit Ctrl-C)")
            should_shutdown = threading.Event()

            def signal_handler(signal, frame):
                should_shutdown.set()

            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)

            while True:
                metrics_reader.load_metrics()
                should_shutdown.wait(timeout=10)
                if should_shutdown.is_set():
                    break

        print("Stopping clients...")
        for _ in range(args.num_clients):
            stop_queue.put("")

    # For printing out results.
    if "COND_OUT" in os.environ:
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(".")

    metrics_df = metrics_reader.get_metrics(metric_ids=METRICS)
    metrics_df.to_csv(out_dir / "metrics.csv")

    # Wait for the experiment to finish.
    for p in processes:
        p.join()

    print("Done!")


if __name__ == "__main__":
    # On Unix platforms, the default way to start a process is by forking, which
    # is not ideal (we do not want to duplicate this process' file
    # descriptors!).
    mp.set_start_method("spawn")
    main()
