import argparse
import multiprocessing as mp
import pyodbc
import queue
import random
import time
import os
import pathlib

from typing import List


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
    conn = pyodbc.connect(cstr)
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
        print("query_idx,run_time_s", file=file)

        # Signal that we're ready to start and wait for the controller.
        start_queue.put_nowait("")
        _ = stop_queue.get()

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
            end = time.time()
            print("{},{}".format(next_query_idx, end - start), file=file)

            try:
                _ = stop_queue.get_nowait()
                break
            except queue.Empty:
                pass


def run_warmup(args):
    cstr = os.environ[args.cstr_var]
    conn = pyodbc.connect(cstr)
    conn.timeout = 30
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
        "--run_for_s", type=int, default=60, help="How long to run the experiment for."
    )
    parser.add_argument("--query_file", type=str, default="queries_30.sql")
    parser.add_argument("--specific_query_idx", type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run_warmup", action="store_true")
    # Controls how the clients submit queries to the underlying engine.
    parser.add_argument("--num_clients", type=int, default=1)
    parser.add_argument("--avg_gap_s", type=float, default=1.0)
    parser.add_argument("--std_gap_s", type=float, default=0.5)
    args = parser.parse_args()

    if args.run_warmup:
        run_warmup(args)
        return

    mgr = mp.Manager()
    start_queue = mgr.Queue()
    stop_queue = mgr.Queue()

    processes = []
    for idx in range(args.num_clients):
        p = mp.Process(target=runner, args=(idx, start_queue, stop_queue, args))
        p.start()
        processes.append(p)

    print("Waiting for startup...")
    for _ in range(args.num_clients):
        start_queue.get()

    print("Telling clients to start.")
    for _ in range(args.num_clients):
        stop_queue.put("")

    print("Letting the experiment run for {} seconds...".format(args.run_for_s))
    time.sleep(args.run_for_s)

    print("Stopping clients...")
    for _ in range(args.num_clients):
        stop_queue.put("")
    for p in processes:
        p.join()

    print("Done!")


if __name__ == "__main__":
    # On Unix platforms, the default way to start a process is by forking, which
    # is not ideal (we do not want to duplicate this process' file
    # descriptors!).
    mp.set_start_method("spawn")
    main()
