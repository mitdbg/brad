import argparse
import multiprocessing as mp
import time
import os
import pathlib
import random
import queue
import sys
import threading
import signal
import pytz
import logging
from typing import List
from datetime import datetime, timedelta

from workload_utils.connect import connect_to_db
from brad.grpc_client import BradClientError
from brad.utils.rand_exponential_backoff import RandomizedExponentialBackoff
from typing import Dict

logger = logging.getLogger(__name__)


def build_query_map(query_bank: str) -> Dict[str, int]:
    queries = []
    with open(query_bank, "r", encoding="UTF-8") as file:
        for line in file:
            query = line.strip()
            if query:
                queries.append(query)

    idx_map = {}
    for idx, q in enumerate(queries):
        idx_map[q] = idx

    return idx_map


def runner(
    runner_idx: int,
    start_queue: mp.Queue,
    stop_queue: mp.Queue,
    args,
    query_bank: List[str],
    queries: List[int],
) -> None:
    def noop(_signal, _frame):
        pass

    signal.signal(signal.SIGINT, noop)

    # For printing out results.
    if "COND_OUT" in os.environ:
        # pylint: disable-next=import-error
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(".")

    database = connect_to_db(args, runner_idx)
    try:
        with open(
            out_dir / "repeating_olap_batch_{}.csv".format(runner_idx),
            "w",
            encoding="UTF-8",
        ) as file:
            print("timestamp,query_idx,run_time_s,engine", file=file, flush=True)

            prng = random.Random(args.seed ^ runner_idx)
            rand_backoff = None

            logger.info(
                "[Repeating Analytics Runner %d] Queries to run: %s",
                runner_idx,
                queries,
            )
            query_order = queries.copy()
            prng.shuffle(query_order)

            # Signal that we're ready to start and wait for the controller.
            start_queue.put_nowait("")
            _ = stop_queue.get()

            while True:
                if args.avg_gap_s is not None:
                    # Wait times are normally distributed right now.
                    # TODO: Consider using a different distribution (e.g., exponential).
                    wait_for_s = prng.gauss(args.avg_gap_s, args.avg_gap_std_s)
                    if wait_for_s < 0.0:
                        wait_for_s = 0.0
                    time.sleep(wait_for_s)

                if len(query_order) == 0:
                    query_order = queries.copy()
                    prng.shuffle(query_order)

                qidx = query_order.pop()
                logger.debug("Executing qidx: %d", qidx)
                query = query_bank[qidx]

                try:
                    engine = None
                    now = datetime.now().astimezone(pytz.utc)
                    start = time.time()
                    _, engine = database.execute_sync_with_engine(query)
                    end = time.time()
                    print(
                        "{},{},{},{}".format(
                            now,
                            qidx,
                            end - start,
                            engine.value if engine is not None else "unknown",
                        ),
                        file=file,
                        flush=True,
                    )
                    rand_backoff = None

                except BradClientError as ex:
                    if ex.is_transient():
                        print(
                            "Transient query error:",
                            ex.message(),
                            flush=True,
                            file=sys.stderr,
                        )

                        if rand_backoff is None:
                            rand_backoff = RandomizedExponentialBackoff(
                                max_retries=10,
                                base_delay_s=2.0,
                                max_delay_s=timedelta(minutes=10).total_seconds(),
                            )

                        # Delay retrying in the case of a transient error (this
                        # happens during blueprint transitions).
                        wait_s = rand_backoff.wait_time_s()
                        if wait_s is None:
                            print("Aborting benchmark. Too many transient errors.")
                            break
                        time.sleep(wait_s)

                    else:
                        print(
                            "Unexpected query error:",
                            ex.message(),
                            flush=True,
                            file=sys.stderr,
                        )

                try:
                    _ = stop_queue.get_nowait()
                    break
                except queue.Empty:
                    pass
    finally:
        database.close_sync()


def run_warmup(args, query_bank: List[str], queries: List[int]):
    database = connect_to_db(args, worker_index=0)

    try:
        with open("repeating_olap_batch_warmup.csv", "w", encoding="UTF-8") as file:
            print("timestamp,query_idx,run_time_s,engine", file=file)
            for idx, qidx in enumerate(queries):
                try:
                    engine = None
                    query = query_bank[qidx]
                    now = datetime.now().astimezone(pytz.utc)
                    start = time.time()
                    _, engine = database.execute_sync_with_engine(query)
                    end = time.time()
                    run_time_s = end - start
                    print(
                        "Warmed up {} of {}. Run time (s): {}".format(
                            idx + 1, len(queries), run_time_s
                        )
                    )
                    print(
                        "{},{},{},{}".format(
                            now,
                            qidx,
                            run_time_s,
                            engine.value if engine is not None else "unknown",
                        ),
                        file=file,
                        flush=True,
                    )
                except BradClientError as ex:
                    if ex.is_transient():
                        print(
                            "Transient query error:",
                            ex.message(),
                            flush=True,
                            file=sys.stderr,
                        )
                    else:
                        print(
                            "Unexpected query error:",
                            ex.message(),
                            flush=True,
                            file=sys.stderr,
                        )
    finally:
        database.close_sync()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brad-host", type=str, default="localhost")
    parser.add_argument("--brad-port", type=int, default=6583)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--num-front-ends", type=int, default=1)
    parser.add_argument("--run-warmup", action="store_true")
    parser.add_argument(
        "--cstr-var",
        type=str,
        help="Set to connect via ODBC instead of the BRAD client (for use with other baselines).",
    )
    parser.add_argument(
        "--query-bank-file", type=str, required=True, help="Path to a query bank."
    )
    parser.add_argument("--num-clients", type=int, default=1)
    parser.add_argument("--avg-gap-s", type=float)
    parser.add_argument("--avg-gap-std-s", type=float, default=0.5)
    parser.add_argument("--query-indexes", type=str, required=True)
    args = parser.parse_args()

    with open(args.query_bank_file, "r", encoding="UTF-8") as file:
        query_bank = [line.strip() for line in file]

    queries = list(map(int, args.query_indexes.split(",")))
    for qidx in queries:
        assert qidx < len(query_bank)
        assert qidx >= 0

    if args.run_warmup:
        run_warmup(args, query_bank, queries)
        return

    mgr = mp.Manager()
    start_queue = mgr.Queue()
    stop_queue = mgr.Queue()

    processes = []
    for idx in range(args.num_clients):
        p = mp.Process(
            target=runner,
            args=(idx, start_queue, stop_queue, args, query_bank, queries),
        )
        p.start()
        processes.append(p)

    print("Waiting for startup...", flush=True)
    for _ in range(args.num_clients):
        start_queue.get()

    print("Telling {} clients to start.".format(args.num_clients), flush=True)
    for _ in range(args.num_clients):
        stop_queue.put("")

    # Wait until requested to stop.
    print(
        "Repeating analytics waiting until requested to stop... (hit Ctrl-C)",
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

    print("Waiting for the clients to complete.")
    for p in processes:
        p.join()

    print("Done!")


if __name__ == "__main__":
    # On Unix platforms, the default way to start a process is by forking, which
    # is not ideal (we do not want to duplicate this process' file
    # descriptors!).
    mp.set_start_method("spawn")
    main()
