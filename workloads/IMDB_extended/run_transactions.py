import asyncio
import argparse
import pathlib
import queue
import random
import signal
import sys
import threading
import time
import os
import pytz
import multiprocessing as mp
from datetime import datetime, timedelta
from typing import Optional

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.grpc_client import BradClientError
from brad.provisioning.directory import Directory
from brad.utils.rand_exponential_backoff import RandomizedExponentialBackoff
from workload_utils.connect import connect_to_db
from workload_utils.transaction_worker import TransactionWorker


def runner(
    args,
    worker_idx: int,
    directory: Optional[Directory],
    start_queue: mp.Queue,
    stop_queue: mp.Queue,
) -> None:
    """
    Meant to be launched as a subprocess with multiprocessing.
    """

    def noop_handler(_signal, _frame):
        pass

    signal.signal(signal.SIGINT, noop_handler)

    worker = TransactionWorker(
        worker_idx, args.seed ^ worker_idx, args.scale_factor, args.dataset_type
    )

    txn_prng = random.Random(~(args.seed ^ worker_idx))
    transactions = [
        worker.purchase_tickets,
        worker.add_new_showing,
        worker.edit_movie_note,
    ]
    transaction_weights = [
        0.70,
        0.20,
        0.10,
    ]
    lookup_theatre_id_by_name = 0.8
    txn_indexes = list(range(len(transactions)))
    commits = [0 for _ in range(len(transactions))]
    aborts = [0 for _ in range(len(transactions))]

    # Connect and set the isolation level.
    db = connect_to_db(
        args, worker_idx, direct_engine=Engine.Aurora, directory=directory
    )
    db.execute_sync(
        f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {args.isolation_level}"
    )

    # For printing out results.
    if "COND_OUT" in os.environ:
        # pylint: disable-next=import-error
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(".")

    # Signal that we are ready to start and wait for other clients.
    start_queue.put("")
    _ = stop_queue.get()

    rand_backoff = None
    overall_start = time.time()
    try:
        latency_file = open(
            out_dir / "oltp_latency_{}.csv".format(worker_idx), "w", encoding="UTF-8"
        )
        print("txn_idx,timestamp,run_time_s", file=latency_file)

        while True:
            txn_idx = txn_prng.choices(txn_indexes, weights=transaction_weights, k=1)[0]
            txn = transactions[txn_idx]

            now = datetime.now().astimezone(pytz.utc)
            txn_start = time.time()
            try:
                # pylint: disable-next=comparison-with-callable
                if txn == worker.purchase_tickets:
                    succeeded = txn(
                        db,
                        select_using_name=txn_prng.random() < lookup_theatre_id_by_name,
                    )
                else:
                    succeeded = txn(db)

                if rand_backoff is not None:
                    print(f"[T {worker_idx}] Continued after transient errors.")
                    rand_backoff = None

            except BradClientError as ex:
                succeeded = False
                if ex.is_transient():
                    # Too verbose during a transition.
                    # print(
                    #     "Encountered transient error (probably engine change). Will retry...",
                    #     flush=True,
                    #     file=sys.stderr,
                    # )

                    if rand_backoff is None:
                        rand_backoff = RandomizedExponentialBackoff(
                            max_retries=100,
                            base_delay_s=0.1,
                            max_delay_s=timedelta(minutes=1).total_seconds(),
                        )
                        print(f"[T {worker_idx}] Backing off due to transient errors.")

                    # Delay retrying in the case of a transient error (this
                    # happens during blueprint transitions).
                    wait_s = rand_backoff.wait_time_s()
                    if wait_s is None:
                        print("Aborting benchmark. Too many transient errors.")
                        break
                    time.sleep(wait_s)

                else:
                    print(
                        "Encountered an unexpected `BradClientError`. Aborting the workload...",
                        flush=True,
                        file=sys.stderr,
                    )
                    raise
            except:
                succeeded = False
                print(
                    "Encountered an unexpected error. Aborting the workload...",
                    flush=True,
                    file=sys.stderr,
                )
                raise
            txn_end = time.time()

            # Record metrics.
            if succeeded:
                commits[txn_idx] += 1
            else:
                aborts[txn_idx] += 1

            if txn_prng.random() < args.latency_sample_prob:
                print(
                    "{},{},{}".format(txn_idx, now, txn_end - txn_start),
                    file=latency_file,
                )

                # Warn if the abort rate is high.
                total_aborts = sum(aborts)
                total_commits = sum(commits)
                abort_rate = total_aborts / (total_aborts + total_commits)
                if abort_rate > 0.15:
                    print(
                        f"[T {worker_idx}] Abort rate is higher than expected ({abort_rate:.4f})."
                    )

            try:
                _ = stop_queue.get_nowait()
                break
            except queue.Empty:
                pass

    finally:
        overall_end = time.time()
        print(f"[{worker_idx}] Done running transactions.", flush=True, file=sys.stderr)
        latency_file.close()

        with open(
            out_dir / "oltp_stats_{}.csv".format(worker_idx), "w", encoding="UTF-8"
        ) as file:
            print("stat,value", file=file)
            print(f"overall_run_time_s,{overall_end - overall_start}", file=file)
            print(f"purchase_commits,{commits[0]}", file=file)
            print(f"add_showing_commits,{commits[1]}", file=file)
            print(f"edit_note_commits,{commits[2]}", file=file)
            print(f"purchase_aborts,{aborts[0]}", file=file)
            print(f"add_showing_aborts,{aborts[1]}", file=file)
            print(f"edit_note_aborts,{aborts[2]}", file=file)

        db.close_sync()


def main():
    parser = argparse.ArgumentParser(
        "Tool used to run IMDB-extended transactions against BRAD or an ODBC database."
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
        help="The number of transactional clients.",
    )
    parser.add_argument("--client-offset", type=int, default=0)
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
    parser.add_argument(
        "--isolation-level",
        type=str,
        default="REPEATABLE READ",
        help="The isolation level to use when running the transactions.",
    )
    parser.add_argument(
        "--brad-direct",
        action="store_true",
        help="Set to connect directly to Aurora via BRAD's config.",
    )
    parser.add_argument(
        "--config-file",
        type=str,
        help="The BRAD config file (if --brad-direct is used).",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        help="The schema name to use, if connecting directly.",
    )
    parser.add_argument(
        "--latency-sample-prob",
        type=float,
        default=0.01,
        help="The probability that a transaction's latency will be recorded.",
    )
    parser.add_argument(
        "--dataset-type",
        choices=["original", "20gb", "100gb"],
        default="original",
        help="This controls the range of reads the transaction worker performs, "
        "depending on the dataset size.",
    )
    parser.add_argument("--brad-host", type=str, default="localhost")
    parser.add_argument("--brad-port", type=int, default=6583)
    parser.add_argument("--num-front-ends", type=int, default=1)
    args = parser.parse_args()

    mgr = mp.Manager()
    start_queue = mgr.Queue()
    stop_queue = mgr.Queue()

    if args.brad_direct:
        assert args.config_file is not None
        assert args.schema_name is not None
        config = ConfigFile.load(args.config_file)
        directory = Directory(config)
        asyncio.run(directory.refresh())
    else:
        directory = None

    clients = []
    for idx in range(args.num_clients):
        p = mp.Process(
            target=runner, args=(args, idx, directory, start_queue, stop_queue)
        )
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
