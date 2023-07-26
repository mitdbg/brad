import argparse
import pathlib
import pyodbc
import queue
import random
import signal
import sys
import threading
import time
import os
import multiprocessing as mp

from brad.grpc_client import BradGrpcClient
from workload_utils.database import Database, PyodbcDatabase, BradDatabase
from workload_utils.geospatial_worker import GeospatialWorker


def runner(
    args,
    worker_idx: int,
    start_queue: mp.Queue,
    stop_queue: mp.Queue,
) -> None:
    """
    Meant to be launched as a subprocess with multiprocessing.
    """

    def noop_handler(_signal, _frame):
        pass

    signal.signal(signal.SIGINT, noop_handler)

    worker = GeospatialWorker(worker_idx, args.seed ^ worker_idx, args.scale_factor)

    prng = random.Random(~(args.seed ^ worker_idx))
    queries = [
        worker.query1,
        worker.query2,
    ]
    query_weights = [0.50, 0.50]
    query_indexes = list(range(len(queries)))
    latencies = [[] for _ in range(len(queries))]

    try:
        # Connect.
        if args.cstr_var is not None:
            db: Database = PyodbcDatabase(
                pyodbc.connect(os.environ[args.cstr_var], autocommit=True)
            )
        else:
            brad = BradGrpcClient(args.brad_host, args.brad_port)
            brad.connect()
            db = BradDatabase(brad)

        # Signal that we are ready to start and wait for other clients.
        start_queue.put("")
        _ = stop_queue.get()

        overall_start = time.time()
        while True:
            q_idx = prng.choices(query_indexes, weights=query_weights, k=1)[0]
            query = queries[q_idx]

            query_start = time.time()
            succeeded = query(db)
            query_end = time.time()

            # Record metrics.
            latencies[q_idx].append(query_end - query_start)

            try:
                _ = stop_queue.get_nowait()
                break
            except queue.Empty:
                pass
        overall_end = time.time()
        print(
            f"[{worker_idx}] Done running geospatial queries.",
            flush=True,
            file=sys.stderr,
        )

    finally:
        # For printing out results.
        if "COND_OUT" in os.environ:
            import conductor.lib as cond

            out_dir = cond.get_output_path()
        else:
            out_dir = pathlib.Path(".")

        with open(
            out_dir / "geospatial_latency_{}.csv".format(worker_idx), "w"
        ) as file:
            print("query_idx,run_time_s", file=file)
            for qidx, lat_list in enumerate(latencies):
                for lat in lat_list:
                    print("{},{}".format(qidx, lat), file=file)

        db.close_sync()


def main():
    parser = argparse.ArgumentParser(
        "Tool used to run IMDB-extended geospatial workload against BRAD or an ODBC database."
    )
    parser.add_argument(
        "--run-for-s",
        type=int,
        help="How long to run the workload for. If unset, the experiment will run until Ctrl-C.",
    )
    parser.add_argument(
        "--num-clients",
        type=int,
        default=1,
        help="The number of geospatial clients.",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for reproducibility."
    )
    parser.add_argument(
        "--cstr-var",
        type=str,
        help="Environment variable that holds a ODBC connection string. Set to connect directly (i.e., not through BRAD)",
    )
    parser.add_argument(
        "--scale-factor",
        type=int,
        default=1,
        help="The scale factor used to generate the dataset.",
    )
    parser.add_argument("--brad-host", type=str, default="localhost")
    parser.add_argument("--brad-port", type=int, default=6583)
    args = parser.parse_args()

    mgr = mp.Manager()
    start_queue = mgr.Queue()
    stop_queue = mgr.Queue()

    clients = []
    for idx in range(args.num_clients):
        p = mp.Process(target=runner, args=(args, idx, start_queue, stop_queue))
        p.start()
        clients.append(p)

    print("Waiting for startup...", file=sys.stderr, flush=True)
    for _ in range(args.num_clients):
        start_queue.get()

    print(
        "Telling {} clients to start.".format(args.num_clients),
        file=sys.stderr,
        flush=True,
    )
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

    print("Waiting for clients to terminate...", flush=True, file=sys.stderr)
    for c in clients:
        c.join()


if __name__ == "__main__":
    # On Unix platforms, the default way to start a process is by forking, which
    # is not ideal (we do not want to duplicate this process' file
    # descriptors!).
    mp.set_start_method("spawn")
    main()
