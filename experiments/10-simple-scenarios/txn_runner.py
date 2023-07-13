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
import sys
import json

from typing import List, Tuple
from datetime import timedelta
from brad.grpc_client import BradGrpcClient
from brad.daemon.perf_insights import AwsPerformanceInsightsClient

BASE_METRICS = [
    "os.loadAverageMinute.one",
    "os.loadAverageMinute.five",
    "os.loadAverageMinute.fifteen",
    "os.cpuUtilization.system",
    "os.cpuUtilization.total",
    "os.cpuUtilization.user",
    "os.diskIO.avgQueueLen",
    "os.diskIO.tps",
    "os.diskIO.util",
    "os.diskIO.readIOsPS",
    "os.diskIO.readKbPS",
    "os.diskIO.writeIOsPS",
    "os.diskIO.writeKbPS",
    "os.network.rx",
    "os.network.tx",
    "os.memory.active",
    "os.memory.dirty",
    "os.memory.free",
    "os.memory.writeback",
    "os.memory.total",
    "os.tasks.blocked",
    "os.tasks.running",
    "os.tasks.sleeping",
    "os.tasks.stopped",
    "os.tasks.total",
    "db.SQL.queries",
    "db.SQL.total_query_time",
    "db.SQL.tup_deleted",
    "db.SQL.tup_fetched",
    "db.SQL.tup_inserted",
    "db.SQL.tup_returned",
    "db.SQL.tup_updated",
    "db.Transactions.active_transactions",
    "db.Transactions.blocked_transactions",
    "db.Transactions.duration_commits",
    "db.Transactions.xact_commit",
    "db.Transactions.xact_rollback",
    # NOTE: Aurora has specific storage metrics (probably because they use a custom storage engine)
    # https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_PerfInsights_Counters.html#USER_PerfInsights_Counters.Aurora_PostgreSQL
    "os.diskIO.auroraStorage.auroraStorageBytesRx",
    "os.diskIO.auroraStorage.auroraStorageBytesTx",
    "os.diskIO.auroraStorage.diskQueueDepth",
    "os.diskIO.auroraStorage.readThroughput",
    "os.diskIO.auroraStorage.writeThroughput",
    "os.diskIO.auroraStorage.readLatency",
    "os.diskIO.auroraStorage.writeLatency",
    "os.diskIO.auroraStorage.readIOsPS",
    "os.diskIO.auroraStorage.writeIOsPS",
]

ALL_METRICS = []
for m in BASE_METRICS:
    # N.B. The metrics are reported no more than once a minute. So
    # average/max/min will all report the same number.
    ALL_METRICS.append(m + ".avg")

TABLE_MAX_IDS = {
    "aka_name": 901343,
    "aka_title": 377960,
    "cast_info": 36244344,
    "char_name": 3140339,
    "comp_cast_type": 4,
    "company_name": 234997,
    "company_type": 4,
    "complete_cast": 135086,
    "info_type": 113,
    "keyword": 134170,
    "kind_type": 7,
    "link_type": 18,
    "movie_companies": 2609129,
    "movie_info": 14835720,
    "movie_info_idx": 1380035,
    "movie_keyword": 4523930,
    "movie_link": 29997,
    "name": 4167491,
    "person_info": 2963664,
    "role_type": 12,
    "title": 2528312,
}


def load_txns(txns_file_path: str, idx: int) -> List[str]:
    # A list of lists of SQL transactions (forming one transaction).
    transactions: List[List[str]] = []

    path = pathlib.Path(txns_file_path)

    with open(path / "transaction_user_{}.json".format(idx + 1), "r") as file:
        parsed = json.load(file)
        for query_seq in parsed.values():
            # The last "query" is a "COMMIT".
            transactions.append(query_seq.split("\n")[:-1])

    return transactions


def load_queries(file_path: str) -> List[str]:
    queries = []
    with open(file_path, "r") as file:
        for line in file:
            query = line.strip()
            if query:
                queries.append(query)
    return queries


def runner(idx: int, start_queue: mp.Queue, stop_queue: mp.Queue, args):
    # Ignore Ctrl-C (we use a different mechanism to shut down.)
    def noop_signal_handler(signal, frame):
        pass

    signal.signal(signal.SIGINT, noop_signal_handler)

    prng = random.Random(args.seed ^ idx)
    txn_list = load_txns(args.txn_dir, idx)
    next_txn_idx = 0

    # Returns the transaction latency.
    def run_txn(client: BradGrpcClient, txn_idx: int) -> Tuple[float, int]:
        txn = txn_list[txn_idx]
        num_aborts = 0

        while True:
            try:
                start = time.time()
                # Explicitly issue BEGIN/COMMIT to mimic how we expect
                # transactions to run against BRAD.
                client.run_query_ignore_results("BEGIN")
                for q in txn:
                    client.run_query_ignore_results(q)
                client.run_query_ignore_results("COMMIT")
                end = time.time()
                break
            except pyodbc.Error:
                # Serialization error.
                client.run_query_ignore_results("ROLLBACK")
                num_aborts += 1
                # Ideally should have backoff logic here. But we do not expect
                # any aborts in our current workload.

        return end - start, num_aborts

    latencies = []
    num_txn_commits = 0
    total_aborts = 0

    brad_client = BradGrpcClient(args.host, args.port)
    brad_client.connect()
    brad_client.run_query_ignore_results(
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {}".format(
            args.isolation_level
        )
    )

    # Signal that we're ready to start and wait for the controller.
    start_queue.put_nowait("")
    _ = stop_queue.get()

    try:
        overall_start = time.time()
        while True:
            if args.add_wait:
                wait_for_s = prng.gauss(args.avg_gap_s, args.std_gap_s)
                if wait_for_s < 0.0:
                    wait_for_s = 0.0
                time.sleep(wait_for_s)

            lat, aborts = run_txn(brad_client, next_txn_idx)
            latencies.append(lat)
            total_aborts += aborts
            next_txn_idx += 1
            next_txn_idx %= len(txn_list)
            num_txn_commits += 1

            try:
                _ = stop_queue.get_nowait()
                break
            except queue.Empty:
                pass
    finally:
        overall_end = time.time()

        # For printing out results.
        if "COND_OUT" in os.environ:
            import conductor.lib as cond

            out_dir = cond.get_output_path()
        else:
            out_dir = pathlib.Path(".")

        with open(out_dir / "oltp_latency_{}.csv".format(idx), "w") as file:
            print("txn_idx,run_time_s", file=file)
            for tidx, lat in enumerate(latencies):
                print("{},{}".format(tidx, lat), file=file)

        with open(out_dir / "oltp_stats_{}.csv".format(idx), "w") as file:
            print("stat,value", file=file)
            print("num_commits,{}".format(num_txn_commits), file=file)
            print(
                "overall_run_time_s,{}".format(overall_end - overall_start), file=file
            )
            print("num_aborts,{}".format(total_aborts), file=file)

        brad_client.close()


def run_warmup(args):
    brad_client = BradGrpcClient(args.host, args.port)
    brad_client.run_query_ignore_results(
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {}".format(
            args.isolation_level
        )
    )

    queries = load_queries(args.warmup_query_file)
    with open("warmup.csv", "w") as file:
        print("query_idx,run_time_s", file=file)
        for idx, q in enumerate(queries):
            try:
                start = time.time()
                brad_client.run_query_ignore_results(q)
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
            except pyodbc.Error as ex:
                print(
                    "Skipping query {} because of an error (potentially timeout)".format(
                        idx
                    ),
                    file=sys.stderr,
                    flush=True,
                )
                print(ex, file=sys.stderr, flush=True)

    brad_client.close()


def trim_tables(args):
    # Used to drop extraneous rows from the tables (inserted by prior transactions).
    with BradGrpcClient(args.host, args.port) as client:
        client.run_query_ignore_results("BEGIN")
        for table, max_orig_id in TABLE_MAX_IDS.items():
            print("Trimming {}...".format(table))
            client.run_query_ignore_results("DELETE FROM {} WHERE id > {}".format(table, max_orig_id))
        print("Committing...")
        client.run_query_ignore_results("COMMIT")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=6583)
    parser.add_argument(
        "--run_for_s",
        type=int,
        help="How long to run the experiment for. If unset, the experiment will run until Ctrl-C.",
    )
    parser.add_argument(
        "--txn_dir", type=str, default="../../workloads/IMDB/OLTP_queries/"
    )
    parser.add_argument("--add_wait", action="store_true")
    parser.add_argument("--warmup_query_file", type=str, default="warmup.sql")
    parser.add_argument("--isolation_level", type=str, default="REPEATABLE READ")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run_warmup", action="store_true")
    parser.add_argument("--run_trim", action="store_true")
    # Controls how the clients submit queries to the underlying engine.
    parser.add_argument("--txn_num_clients", type=int, default=1)
    parser.add_argument("--avg_gap_s", type=float, default=1.0)
    parser.add_argument("--std_gap_s", type=float, default=0.5)
    parser.add_argument(
        "--aurora_cluster", type=str, default="aurora-2"
    )
    parser.add_argument(
        "--aurora_instance", type=str, default="aurora-2-instance-1"
    )
    parser.add_argument("--wait_before_start", type=int)
    args, _ = parser.parse_known_args()

    if args.run_trim:
        trim_tables(args)
        return

    if args.run_warmup:
        run_warmup(args)
        return

    pi_client = AwsPerformanceInsightsClient(args.aurora_instance)
    # Sanity check
    _ = pi_client.fetch_metrics(
        ALL_METRICS, period=timedelta(seconds=60), num_prev_points=10
    )

    if args.wait_before_start is not None:
        print(
            "Waiting {} seconds before starting...".format(args.wait_before_start),
            flush=True,
        )
        time.sleep(args.wait_before_start)

    mgr = mp.Manager()
    start_queue = mgr.Queue()
    stop_queue = mgr.Queue()

    processes = []
    for idx in range(args.txn_num_clients):
        p = mp.Process(target=runner, args=(idx, start_queue, stop_queue, args))
        p.start()
        processes.append(p)

    print("Waiting for startup...", flush=True)
    for _ in range(args.txn_num_clients):
        start_queue.get()

    print("Telling {} clients to start.".format(args.txn_num_clients), flush=True)
    for _ in range(args.txn_num_clients):
        stop_queue.put("")

    if args.run_for_s is not None:
        print(
            "Letting the experiment run for {} seconds...".format(args.run_for_s),
            flush=True,
        )
        time.sleep(args.run_for_s)

    else:
        print("Waiting until requested to stop... (hit Ctrl-C)")
        should_shutdown = threading.Event()

        def signal_handler(signal, frame):
            should_shutdown.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        should_shutdown.wait()

    print("Stopping clients...")
    for _ in range(args.txn_num_clients):
        stop_queue.put("")

    # For printing out results.
    if "COND_OUT" in os.environ:
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(".")

    print("Waiting a few seconds before retrieving metrics...", file=sys.stderr)
    time.sleep(20)

    instance_metrics = pi_client.fetch_metrics(
        ALL_METRICS, period=timedelta(seconds=60), num_prev_points=10
    )
    instance_metrics.to_csv(out_dir / "metrics.csv")

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
