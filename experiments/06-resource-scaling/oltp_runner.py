import argparse
import multiprocessing as mp
import pyodbc
import queue
import random
import time
import os
import pathlib
import json

from typing import List, Tuple

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


def runner(idx: int, start_queue: mp.Queue, stop_queue: mp.Queue, args):
    cstr = os.environ[args.cstr_var]
    conn = pyodbc.connect(cstr)
    cursor = conn.cursor()
    cursor.execute(
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {}".format(
            args.isolation_level
        )
    )

    txn_list = load_txns(args.transaction_path, idx)
    next_txn_idx = 0

    prng = random.Random(args.seed ^ idx)

    # Signal that we're ready to start and wait for the controller.
    start_queue.put_nowait("")
    _ = stop_queue.get()

    # Returns the transaction latency.
    def run_txn(txn_idx: int) -> Tuple[float, int]:
        txn = txn_list[txn_idx]
        num_aborts = 0

        while True:
            try:
                start = time.time()
                for q in txn:
                    cursor.execute(q)
                cursor.commit()
                end = time.time()
                break
            except pyodbc.Error:
                # Serialization error.
                cursor.rollback()
                num_aborts += 1

        return end - start, num_aborts

    latencies = []
    num_txns_executed = 0
    total_aborts = 0

    overall_start = time.time()
    while True:
        if args.add_wait:
            wait_for_s = prng.gauss(args.avg_gap_s, args.std_gap_s)
            if wait_for_s < 0.0:
                wait_for_s = 0.0
            time.sleep(wait_for_s)

        lat, aborts = run_txn(next_txn_idx)
        latencies.append(lat)
        total_aborts += aborts
        next_txn_idx += 1
        next_txn_idx %= len(txn_list)
        num_txns_executed += 1

        try:
            _ = stop_queue.get_nowait()
            break
        except queue.Empty:
            pass
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
        print("num_txns,{}".format(num_txns_executed), file=file)
        print("overall_run_time_s,{}".format(overall_end - overall_start), file=file)
        print("num_aborts,{}".format(total_aborts), file=file)


def trim_tables(args):
    # Used to drop extraneous rows from the tables (inserted by prior transactions).
    cstr = os.environ[args.cstr_var]
    conn = pyodbc.connect(cstr)
    cursor = conn.cursor()

    for table, max_orig_id in TABLE_MAX_IDS.items():
        print("Trimming {}...".format(table))
        cursor.execute("DELETE FROM {} WHERE id > {}".format(table, max_orig_id))

    cursor.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cstr_var", type=str, required=True, help="The ODBC connection string"
    )
    parser.add_argument(
        "--run_for_s", type=int, default=60, help="How long to run the experiment for."
    )
    parser.add_argument("--transaction_path", type=str, required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--trim_only", action="store_true")
    parser.add_argument("--isolation_level", type=str, default="SERIALIZABLE")
    # Controls how the clients submit queries to the underlying engine.
    parser.add_argument("--num_clients", type=int, default=1)
    parser.add_argument("--add_wait", action="store_true")
    parser.add_argument("--avg_gap_s", type=float, default=1.0)
    parser.add_argument("--std_gap_s", type=float, default=0.5)
    args = parser.parse_args()

    trim_tables(args)
    if args.trim_only:
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
