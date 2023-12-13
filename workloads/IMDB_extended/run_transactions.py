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
import pickle
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

EXECUTE_START_TIME = datetime.now().astimezone(pytz.utc)


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

    if args.baseline is not None and args.baseline != "":
        # Hack.
        # TODO: Reset to 100gb.
        dataset_type = "original"
    else:
        dataset_type = "original"
    worker = TransactionWorker(
        worker_idx, args.seed ^ worker_idx, args.scale_factor, dataset_type=dataset_type
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

    if args.baseline != "tidb":
        db.execute_sync(
            f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {args.isolation_level}"
        )

    # For printing out results.
    if "COND_OUT" in os.environ:
        # pylint: disable-next=import-error
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(f"./{args.output_dir}").resolve()

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

                rand_backoff = None

            except BradClientError as ex:
                succeeded = False
                if ex.is_transient():
                    print(
                        "Encountered transient error (probably engine change). Will retry...",
                        flush=True,
                        file=sys.stderr,
                    )

                    if rand_backoff is None:
                        rand_backoff = RandomizedExponentialBackoff(
                            max_retries=100,
                            base_delay_s=0.1,
                            max_delay_s=timedelta(minutes=1).total_seconds(),
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
    parser.add_argument(
        "--num-client-path",
        type=str,
        default=None,
        help="Path to the distribution of number of clients for each period of a day",
    )
    parser.add_argument(
        "--num-client-multiplier",
        type=int,
        default=2,
        help="The multiplier to the number of clients for each period of a day",
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
        "--baseline",
        default="",
        type=str,
        help="Whether to use tidb, aurora or redshift",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="Environment variable that stores the output directory of the results",
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
        "--time-scale-factor",
        type=int,
        default=50,
        help="trace 1s of simulation as X seconds in real-time to match the num-concurrent-query",
    )
    parser.add_argument("--brad-host", type=str, default="localhost")
    parser.add_argument("--brad-port", type=int, default=6583)
    parser.add_argument("--num-front-ends", type=int, default=1)
    args = parser.parse_args()

    if args.brad_direct:
        assert args.config_file is not None
        assert args.schema_name is not None
        config = ConfigFile.load(args.config_file)
        directory = Directory(config)
        asyncio.run(directory.refresh())
    else:
        directory = None

    if (
        args.num_client_path is not None
        and os.path.exists(args.num_client_path)
        and args.time_scale_factor is not None
    ):
        # we can only set the num_concurrent_query trace in presence of time_scale_factor
        with open(args.num_client_path, "rb") as f:
            num_client_trace = pickle.load(f)
        # Multiply each client with multiplier
        multiplier = args.num_client_multiplier
        num_client_trace = {t: n * multiplier for t, n in num_client_trace.items()}
        # Replace args.num_clients with maximum number of clients in trace.
        args.num_clients = max(num_client_trace.values())
    else:
        num_client_trace = None

    mgr = mp.Manager()
    start_queue = [mgr.Queue() for _ in range(args.num_clients)]
    stop_queue = [mgr.Queue() for _ in range(args.num_clients)]

    processes = []
    for idx in range(args.num_clients):
        p = mp.Process(
            target=runner,
            args=(args, idx, directory, start_queue[idx], stop_queue[idx]),
        )
        p.start()
        processes.append(p)

    print("Waiting for startup...", file=sys.stderr, flush=True)
    for idx in range(args.num_clients):
        start_queue[idx].get()

    global EXECUTE_START_TIME  # pylint: disable=global-statement
    EXECUTE_START_TIME = datetime.now().astimezone(
        pytz.utc
    )  # pylint: disable=global-statement

    if num_client_trace is not None:
        assert args.time_scale_factor is not None, "need to set args.time_scale_factor"
        print("[Transactions] Telling client no.{} to start.".format(0), flush=True)
        stop_queue[0].put("")
        num_running_client = 1

        finished_one_day = True
        curr_day_start_time = datetime.now().astimezone(pytz.utc)
        for time_of_day in num_client_trace:
            if time_of_day == 0:
                continue
            # at this time_of_day start/shut-down more clients
            time_in_s = time_of_day / args.time_scale_factor
            now = datetime.now().astimezone(pytz.utc)
            curr_time_in_s = (now - curr_day_start_time).total_seconds()
            total_exec_time_in_s = (now - EXECUTE_START_TIME).total_seconds()
            if args.run_for_s <= total_exec_time_in_s:
                finished_one_day = False
                break
            if args.run_for_s - total_exec_time_in_s <= (time_in_s - curr_time_in_s):
                wait_time = args.run_for_s - total_exec_time_in_s
                if wait_time > 0:
                    time.sleep(wait_time)
                finished_one_day = False
                break
            time.sleep(time_in_s - curr_time_in_s)
            num_client_required = min(num_client_trace[time_of_day], args.num_clients)
            if num_client_required > num_running_client:
                # starting additional clients
                for add_client in range(num_running_client, num_client_required):
                    print(
                        "[Transactions] Telling client no.{} to start.".format(
                            add_client
                        ),
                        flush=True,
                    )
                    stop_queue[add_client].put("")
                    num_running_client += 1
            elif num_running_client > num_client_required:
                # shutting down clients
                for delete_client in range(num_running_client, num_client_required, -1):
                    print(
                        "[Transactions] Telling client no.{} to stop.".format(
                            delete_client - 1
                        ),
                        flush=True,
                    )
                    stop_queue[delete_client - 1].put("")
                    num_running_client -= 1
        now = datetime.now().astimezone(pytz.utc)
        total_exec_time_in_s = (now - EXECUTE_START_TIME).total_seconds()
        if finished_one_day:
            print(
                f"[Transactions] Finished executing one day of workload in {total_exec_time_in_s}s, will ignore the rest of "
                f"pre-set execution time {args.run_for_s}s"
            )
        else:
            print(
                f"[Transactions] Executed ended but unable to finish executing the trace of a full day within {args.run_for_s}s"
            )

    else:
        print(
            "[Transactions] Telling all {} clients to start.".format(args.num_clients),
            flush=True,
        )
        for i in range(args.num_clients):
            stop_queue[i].put("")

    if args.run_for_s and num_client_trace is None:
        print(
            "[Transactions] Waiting for {} seconds...".format(args.run_for_s),
            flush=True,
            file=sys.stderr,
        )
        time.sleep(args.run_for_s)
    elif num_client_trace is None:
        # Wait until requested to stop.
        print(
            "[Transactions] Waiting until requested to stop... (hit Ctrl-C)",
            flush=True,
            file=sys.stderr,
        )
        should_shutdown = threading.Event()

        def signal_handler(_signal, _frame):
            should_shutdown.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        should_shutdown.wait()

    print("Stopping all clients...", flush=True, file=sys.stderr)
    for i in range(args.num_clients):
        stop_queue[i].put("")

    # stop again, just incase some client hasn't started yet
    for i in range(args.num_clients):
        stop_queue[i].put("")

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
