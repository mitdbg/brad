import argparse
import multiprocessing as mp
import time
import os
import pathlib
import random
import sys
import signal
import pytz
import logging
from typing import List
from datetime import datetime, timedelta

from workload_utils.connect import connect_to_db
from brad.config.engine import Engine
from brad.grpc_client import BradClientError
from brad.utils.rand_exponential_backoff import RandomizedExponentialBackoff


logger = logging.getLogger(__name__)

STARTUP_FAILED = "startup_failed"


def runner(
    runner_idx: int,
    start_queue: mp.Queue,
    control_semaphore: mp.Semaphore,  # type: ignore
    args,
    queries: List[str],
) -> None:
    # Check args.
    assert args.num_clients > runner_idx

    def noop(_signal, _frame):
        pass

    signal.signal(signal.SIGINT, noop)

    # For printing out results.
    if "COND_OUT" in os.environ:
        # pylint: disable-next=import-error
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(f"./{args.output_dir}").resolve()

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
        print(f"[Seq runner {runner_idx}] Failed to connect to BRAD:", str(ex))
        start_queue.put_nowait(STARTUP_FAILED)
        return

    # Query indexes the runner should execute.
    runner_qidx = [i for i in range(len(queries)) if i % args.num_clients == runner_idx]

    exec_count = 0
    file = open(
        out_dir / "seq_queries_{}.csv".format(runner_idx),
        "w",
        encoding="UTF-8",
    )

    try:
        print(
            "timestamp,query_idx,run_time_s,engine",
            file=file,
            flush=True,
        )

        prng = random.Random(args.seed ^ runner_idx)
        rand_backoff = None

        logger.info(
            "[Ad hoc Runner %d] Queries to run: %s",
            runner_idx,
            queries,
        )

        # Signal that we're ready to start and wait for the controller.
        print(
            f"Seq Runner {runner_idx} is ready to start running.",
            flush=True,
            file=sys.stderr,
        )
        start_queue.put_nowait("")
        control_semaphore.acquire()  # type: ignore

        last_run_time_s = None

        for qidx in runner_qidx:
            # Note that `False` means to not block.
            should_exit_early = control_semaphore.acquire(False)  # type: ignore
            if should_exit_early:
                print(
                    f"Seq Runner {runner_idx} is exiting early.",
                    file=sys.stderr,
                    flush=True,
                )
                break

            # Wait for some time before issuing, if requested.
            if args.avg_gap_s is not None:
                wait_for_s = prng.gauss(args.avg_gap_s, args.avg_gap_std_s)
            elif args.arrivals_per_s is not None:
                wait_for_s = prng.expovariate(args.arrivals_per_s)
                if last_run_time_s is not None:
                    wait_for_s -= last_run_time_s
            else:
                wait_for_s = 0.0

            if wait_for_s > 0.0:
                time.sleep(wait_for_s)

            logger.debug("Executing qidx: %d", qidx)
            query = queries[qidx]

            try:
                # Get time stamp for logging.
                now = datetime.now().astimezone(pytz.utc)

                # Execute query.
                start = time.time()
                _, engine = database.execute_sync_with_engine(query)
                # Log.
                engine_log = "xxx"
                if engine is None:
                    engine_log = "tidb"
                elif isinstance(engine, Engine):
                    engine_log = engine.value
                else:
                    engine_log = engine
                end = time.time()
                run_time_s = end - start
                print(
                    "{},{},{},{}".format(
                        now,
                        qidx,
                        run_time_s,
                        engine_log,
                    ),
                    file=file,
                    flush=True,
                )
                last_run_time_s = run_time_s

                if exec_count % 20 == 0:
                    # To avoid data loss if this script crashes.
                    os.fsync(file.fileno())

                exec_count += 1
                if rand_backoff is not None:
                    print(
                        f"[Seq Runner {runner_idx}] Continued after transient errors.",
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
                            f"[Seq Runner {runner_idx}] Backing off due to transient errors.",
                            flush=True,
                            file=sys.stderr,
                        )

                    # Delay retrying in the case of a transient error (this
                    # happens during blueprint transitions).
                    wait_s = rand_backoff.wait_time_s()
                    if wait_s is None:
                        print(
                            f"[Seq Runner {runner_idx}] Aborting benchmark. Too many transient errors.",
                            flush=True,
                            file=sys.stderr,
                        )
                        break
                    time.sleep(wait_s)

                else:
                    print(
                        "Unexpected seq query error:",
                        ex.message(),
                        flush=True,
                        file=sys.stderr,
                    )

    finally:
        os.fsync(file.fileno())
        file.close()
        database.close_sync()
        print(f"Seq runner {runner_idx} has exited.", flush=True, file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brad-host", type=str, default="localhost")
    parser.add_argument("--brad-port", type=int, default=6583)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--num-front-ends", type=int, default=1)
    parser.add_argument(
        "--cstr-var",
        type=str,
        help="Set to connect via ODBC instead of the BRAD client (for use with other baselines).",
    )
    parser.add_argument(
        "--query-sequence-file",
        type=str,
        required=True,
        help="Path to a query sequence.",
    )
    # Use these to slice the file, if needed.
    parser.add_argument("--query-sequence-offset", type=int, default=0)
    parser.add_argument("--query-sequence-length", type=int)
    parser.add_argument("--num-clients", type=int, default=1)
    parser.add_argument("--client-offset", type=int, default=0)
    parser.add_argument("--avg-gap-s", type=float)
    parser.add_argument("--avg-gap-std-s", type=float, default=0.5)
    # Set this to use an exponential distribution for the gap times.\
    # This value is per-client.
    parser.add_argument("--arrivals-per-s", type=float)
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
    parser.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="Environment variable that stores the output directory of the results",
    )
    parser.add_argument(
        "--baseline",
        default="",
        type=str,
        help="Whether to use tidb, aurora or redshift",
    )
    args = parser.parse_args()

    with open(args.query_sequence_file, "r", encoding="UTF-8") as file:
        query_seq = [line.strip() for line in file]

        # Truncate according to requested offset and sequence length.
        offset = args.query_sequence_offset
        seq_len = args.query_sequence_length
        query_seq = query_seq[offset:]
        if seq_len is not None:
            query_seq = query_seq[:seq_len]

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
                query_seq,
            ),
        )
        p.start()
        processes.append(p)

    print("Seq: Waiting for startup...", flush=True)
    one_startup_failed = False
    for i in range(args.num_clients):
        msg = start_queue[i].get()
        if msg == STARTUP_FAILED:
            one_startup_failed = True

    if one_startup_failed:
        print("At least one ad-hoc runner failed to start up. Aborting the experiment.")
        for i in range(args.num_clients):
            # Ideally we should be able to release twice atomically.
            control_semaphore[i].release()
            control_semaphore[i].release()
        for p in processes:
            p.join()
        print("Abort complete.")
        return

    print(
        "Telling all {} ad-hoc clients to start.".format(args.num_clients), flush=True
    )
    for i in range(args.num_clients):
        control_semaphore[i].release()

    # Wait until requested to stop.
    print(
        "Seq queries running until completion. Hit Ctrl-C to stop early.",
        flush=True,
        file=sys.stderr,
    )

    def signal_handler(_signal, _frame):
        for i in range(args.num_clients):
            control_semaphore[i].release()
            control_semaphore[i].release()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("Waiting for the seq clients to complete...", flush=True, file=sys.stderr)
    for p in processes:
        p.join()

    print("Done query sequence!", flush=True, file=sys.stderr)


if __name__ == "__main__":
    # On Unix platforms, the default way to start a process is by forking, which
    # is not ideal (we do not want to duplicate this process' file
    # descriptors!).
    mp.set_start_method("spawn")
    main()
