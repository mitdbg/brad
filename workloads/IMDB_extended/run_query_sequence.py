import argparse
import copy
import multiprocessing as mp
import time
import os
import pickle
import numpy as np
import pathlib
import random
import sys
import threading
import signal
import pytz
import logging
from typing import List, Optional
import numpy.typing as npt
from datetime import datetime, timedelta

from workload_utils.connect import connect_to_db
from brad.config.engine import Engine
from brad.grpc_client import BradClientError
from brad.utils.rand_exponential_backoff import RandomizedExponentialBackoff

logger = logging.getLogger(__name__)
EXECUTE_START_TIME = datetime.now().astimezone(pytz.utc)
ENGINE_NAMES = ["ATHENA", "AURORA", "REDSHIFT"]

STARTUP_FAILED = "startup_failed"


def runner(
    runner_idx: int,
    start_queue: mp.Queue,
    control_semaphore: mp.Semaphore,  # type: ignore
    args,
    queries: List[str],
    query_frequency: Optional[npt.NDArray] = None,
    execution_gap_dist: Optional[npt.NDArray] = None,
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

    if args.engine is not None:
        engine = Engine.from_str(args.engine)
    else:
        engine = None

    try:
        database = connect_to_db(
            args,
            runner_idx,
            direct_engine=engine,
            # Ensure we disable the result cache if we are running directly on
            # Redshift.
            disable_direct_redshift_result_cache=True,
        )
    except BradClientError as ex:
        print(f"[RA {runner_idx}] Failed to connect to BRAD:", str(ex))
        start_queue.put_nowait(STARTUP_FAILED)
        return

    if query_frequency is not None:
        query_frequency = query_frequency[queries]
        query_frequency = query_frequency / np.sum(query_frequency)

    exec_count = 0
    file = open(
        out_dir / "repeating_olap_batch_{}.csv".format(runner_idx),
        "w",
        encoding="UTF-8",
    )

    try:
        print(
            "timestamp,time_since_execution_s,time_of_day,query_idx,run_time_s,engine",
            file=file,
            flush=True,
        )

        prng = random.Random(args.seed ^ runner_idx)
        rand_backoff = None

        logger.info(
            "[Repeating Analytics Runner %d] Queries to run: %s",
            runner_idx,
            queries,
        )
        query_order_main = queries.copy()
        prng.shuffle(query_order_main)
        query_order = query_order_main.copy()

        # Signal that we're ready to start and wait for the controller.
        print(
            f"Runner {runner_idx} is ready to start running.",
            flush=True,
            file=sys.stderr,
        )
        start_queue.put_nowait("")
        control_semaphore.acquire()  # type: ignore

        while True:
            # Note that `False` means to not block.
            should_exit = control_semaphore.acquire(False)  # type: ignore
            if should_exit:
                print(f"Runner {runner_idx} is exiting.", file=sys.stderr, flush=True)
                break

            if execution_gap_dist is not None:
                now = datetime.now().astimezone(pytz.utc)
                time_unsimulated = get_time_of_the_day_unsimulated(
                    now, args.time_scale_factor
                )
                wait_for_s = execution_gap_dist[
                    int(time_unsimulated / (60 * 24) * len(execution_gap_dist))
                ]
                time.sleep(wait_for_s)
            elif args.avg_gap_s is not None:
                # Wait times are normally distributed if execution_gap_dist is not provided.
                wait_for_s = prng.gauss(args.avg_gap_s, args.avg_gap_std_s)
                if wait_for_s < 0.0:
                    wait_for_s = 0.0
                time.sleep(wait_for_s)

            if query_frequency is not None:
                qidx = prng.choices(queries, list(query_frequency))[0]
            else:
                if len(query_order) == 0:
                    query_order = query_order_main.copy()

                qidx = query_order.pop()
            logger.debug("Executing qidx: %d", qidx)
            query = query_bank[qidx]

            try:
                engine = None
                now = datetime.now().astimezone(pytz.utc)
                if args.time_scale_factor is not None:
                    time_unsimulated = get_time_of_the_day_unsimulated(
                        now, args.time_scale_factor
                    )
                    time_unsimulated_str = time_in_minute_to_datetime_str(
                        time_unsimulated
                    )
                else:
                    time_unsimulated_str = "xxx"

                start = time.time()
                _, engine = database.execute_sync_with_engine(query)
                end = time.time()
                print(
                    "{},{},{},{},{},{}".format(
                        now,
                        (now - EXECUTE_START_TIME).total_seconds(),
                        time_unsimulated_str,
                        qidx,
                        end - start,
                        engine.value,
                    ),
                    file=file,
                    flush=True,
                )

                if exec_count % 20 == 0:
                    # To avoid data loss if this script crashes.
                    os.fsync(file.fileno())

                exec_count += 1
                if rand_backoff is not None:
                    print(
                        f"[RA {runner_idx}] Continued after transient errors.",
                        flush=True,
                        file=sys.stderr,
                    )
                    rand_backoff = None

            except BradClientError as ex:
                if ex.is_transient():
                    # This is too verbose during a transition.
                    # print(
                    #     "Transient query error:",
                    #     ex.message(),
                    #     flush=True,
                    #     file=sys.stderr,
                    # )

                    if rand_backoff is None:
                        rand_backoff = RandomizedExponentialBackoff(
                            max_retries=100,
                            base_delay_s=1.0,
                            max_delay_s=timedelta(minutes=1).total_seconds(),
                        )
                        print(
                            f"[RA {runner_idx}] Backing off due to transient errors.",
                            flush=True,
                            file=sys.stderr,
                        )

                    # Delay retrying in the case of a transient error (this
                    # happens during blueprint transitions).
                    wait_s = rand_backoff.wait_time_s()
                    if wait_s is None:
                        print(
                            f"[RA {runner_idx}] Aborting benchmark. Too many transient errors.",
                            flush=True,
                            file=sys.stderr,
                        )
                        break
                    time.sleep(wait_s)

                else:
                    print(
                        "Unexpected query error:",
                        ex.message(),
                        flush=True,
                        file=sys.stderr,
                    )

    finally:
        os.fsync(file.fileno())
        file.close()
        database.close_sync()
        print(f"Runner {runner_idx} has exited.", flush=True, file=sys.stderr)


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
        "--query-sequence-file", type=str, required=True, help="Path to a query sequence."
    )
    parser.add_argument("--query-sequence-offset", type=int, default=0)
    parser.add_argument("--query-sequence-length", type=int)  # By default, use the whole file.
    parser.add_argument("--num-clients", type=int, default=1)
    parser.add_argument("--client-offset", type=int, default=0)
    parser.add_argument("--per-client-rate-per-query", type=float, default=0)
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
        "--engine", type=str, help="The engine to use, if connecting directly."
    )
    parser.add_argument("--run-for-s", type=int, help="If set, run for this long.")
    args = parser.parse_args()

    with open(args.query_sequence_file, "r", encoding="UTF-8") as file:
        query_seq = [line.strip() for line in file]

    # Our control protocol is as follows.
    # - Runner processes write to their `start_queue` when they have finished
    #   setting up and are ready to start running. They then wait on the control
    #   semaphore.
    # - The control process blocks and waits on each `start_queue` to ensure
    #   runners can start together (if needed).
    # - The control process signals the control semaphore twice. Once to tell a
    #   runner to start, once to tell it to stop.
    # - If there is an error, a runner is free to exit as long as they have
    #   written to `start_queue`.
    mgr = mp.Manager()
    start_queue = [mgr.Queue() for _ in range(args.num_clients)]
    # N.B. `value = 0` since we use this for synchronization, not mutual exclusion.
    # pylint: disable-next=no-member
    control_semaphore = [mgr.Semaphore(value=0) for _ in range(args.num_clients)]

    processes = []
    for idx in range(args.num_clients):
        p = mp.Process(
            target=runner,
            args=(
                idx,
                start_queue[idx],
                control_semaphore[idx],
                args,
                queries,
            ),
        )
        p.start()
        processes.append(p)

    print("Waiting for startup...", flush=True)
    one_startup_failed = False
    for i in range(args.num_clients):
        msg = start_queue[i].get()
        if msg == STARTUP_FAILED:
            one_startup_failed = True

    if one_startup_failed:
        print("At least one runner failed to start up. Aborting the experiment.")
        for i in range(args.num_clients):
            # Ideally we should be able to release twice atomically.
            control_semaphore[i].release()
            control_semaphore[i].release()
        for p in processes:
            p.join()
        print("Abort complete.")
        return

    print("Telling all {} clients to start.".format(args.num_clients), flush=True)
    for i in range(args.num_clients):
        control_semaphore[i].release()

    if args.run_for_s is not None:
        print(
            "Waiting for {} seconds...".format(args.run_for_s),
            flush=True,
            file=sys.stderr,
        )
        time.sleep(args.run_for_s)
    else:
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

    print("Stopping all clients...", flush=True, file=sys.stderr)
    for i in range(args.num_clients):
        # Note that in most cases, one release will have already run. This is OK
        # because downstream runners will not hang if there is a unconsumed
        # semaphore value.
        control_semaphore[i].release()
        control_semaphore[i].release()

    print("Waiting for the clients to complete...", flush=True, file=sys.stderr)
    for p in processes:
        p.join()

    for idx, p in enumerate(processes):
        print(f"Runner {idx} exit code:", p.exitcode, flush=True, file=sys.stderr)

    print("Done query sequence!", flush=True, file=sys.stderr)


if __name__ == "__main__":
    # On Unix platforms, the default way to start a process is by forking, which
    # is not ideal (we do not want to duplicate this process' file
    # descriptors!).
    mp.set_start_method("spawn")
    main()
