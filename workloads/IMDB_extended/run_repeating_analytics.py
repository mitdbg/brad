import argparse
import copy
import multiprocessing as mp
import time
import os
import pickle
import numpy as np
import pathlib
import random
import queue
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


def get_time_of_the_day_unsimulated(
    now: datetime, time_scale_factor: Optional[int]
) -> int:
    # Get the time of the day in minute in real-time
    assert time_scale_factor is not None, "need to specify args.time_scale_factor"
    # time_diff in minutes after scaling
    time_diff = int((now - EXECUTE_START_TIME).total_seconds() / 60 * time_scale_factor)
    time_unsimulated = time_diff % (24 * 60)  # time of the day in minutes
    return time_unsimulated


def time_in_minute_to_datetime_str(time_unsimulated: Optional[int]) -> str:
    if time_unsimulated is None:
        return "xxx"
    hour = time_unsimulated // 60
    assert hour < 24
    minute = time_unsimulated % 60
    hour_str = str(hour) if hour >= 10 else "0" + str(hour)
    minute_str = str(minute) if minute >= 10 else "0" + str(minute)
    return f"{hour_str}:{minute_str}"


def runner(
    runner_idx: int,
    start_queue: mp.Queue,
    stop_queue: mp.Queue,
    args,
    query_bank: List[str],
    queries: List[int],
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

    database = connect_to_db(
        args,
        runner_idx,
        direct_engine=engine,
        # Ensure we disable the result cache if we are running directly on
        # Redshift.
        disable_direct_redshift_result_cache=True,
    )

    if query_frequency is not None:
        query_frequency = query_frequency[queries]
        query_frequency = query_frequency / np.sum(query_frequency)

    try:
        with open(
            out_dir / "repeating_olap_batch_{}.csv".format(runner_idx),
            "w",
            encoding="UTF-8",
        ) as file:
            print(
                "timestamp,time_since_execution,time_of_day,query_idx,run_time,engine",
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
            query_order = queries.copy()
            prng.shuffle(query_order)

            # Signal that we're ready to start and wait for the controller.
            start_queue.put_nowait("")
            _ = stop_queue.get()

            while True:
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
                        query_order = queries.copy()
                        prng.shuffle(query_order)

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


def simulation_runner(
    all_query_runtime: npt.NDArray,
    runner_idx: int,
    start_queue: mp.Queue,
    stop_queue: mp.Queue,
    args,
    query_bank: List[str],
    queries: List[int],
    query_frequency_original: Optional[npt.NDArray] = None,
    execution_gap_dist: Optional[npt.NDArray] = None,
    wait_for_execute: bool = False,
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

    if query_frequency_original is not None:
        query_frequency = copy.deepcopy(query_frequency_original[queries])
        query_frequency = query_frequency / np.sum(query_frequency)
    else:
        query_frequency = None

    with open(
        out_dir / "repeating_olap_batch_{}.csv".format(runner_idx),
        "w",
        encoding="UTF-8",
    ) as file:
        print(
            "timestamp,time_since_execution,time_of_day,query_idx,run_time_s,engine",
            file=file,
            flush=True,
        )

        prng = random.Random(args.seed ^ runner_idx)

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
                    query_order = queries.copy()
                    prng.shuffle(query_order)

                qidx = query_order.pop()
            logger.debug("Executing qidx: %d", qidx)
            query = query_bank[qidx]
            # using the average of the best two engines as approximation of brad runtime
            runtime = (
                np.sum(all_query_runtime[qidx]) - np.min(all_query_runtime[qidx])
            ) / 2
            if wait_for_execute:
                time.sleep(runtime)
            engine = np.argmin(all_query_runtime[qidx])

            now = datetime.now().astimezone(pytz.utc)
            if args.time_scale_factor is not None:
                time_unsimulated = get_time_of_the_day_unsimulated(
                    now, args.time_scale_factor
                )
                time_unsimulated_str = time_in_minute_to_datetime_str(time_unsimulated)
            else:
                time_unsimulated_str = "xxx"
            print(
                "{},{},{},{},{},{}".format(
                    now,
                    (now - EXECUTE_START_TIME).total_seconds(),
                    time_unsimulated_str,
                    qidx,
                    runtime,
                    ENGINE_NAMES[engine],
                ),
                file=file,
                flush=True,
            )

            try:
                _ = stop_queue.get_nowait()
                break
            except queue.Empty:
                pass


def run_warmup(args, query_bank: List[str], queries: List[int]):
    if args.engine is not None:
        engine = Engine.from_str(args.engine)
    else:
        engine = None

    database = connect_to_db(
        args,
        worker_index=0,
        direct_engine=engine,
        # Ensure we disable the result cache if we are running directly on
        # Redshift.
        disable_direct_redshift_result_cache=True,
    )

    # For printing out results.
    if "COND_OUT" in os.environ:
        # pylint: disable-next=import-error
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(".")

    try:
        print(
            f"Starting warmup pass (will run {args.run_warmup_times} times)...",
            file=sys.stderr,
            flush=True,
        )
        with open(
            out_dir / "repeating_olap_batch_warmup.csv", "w", encoding="UTF-8"
        ) as file:
            print("timestamp,query_idx,run_time_s,engine", file=file)
            for _ in range(args.run_warmup_times):
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
                            ),
                            file=sys.stderr,
                            flush=True,
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
        "--run-simulation",
        action="store_true",
        help="Run the simulation instead of actual execution.",
    )
    parser.add_argument(
        "--wait-for-execute-sim",
        action="store_true",
        help="Waiting for execution in simulation?",
    )
    parser.add_argument(
        "--query-runtime-path",
        type=str,
        default=None,
        help="path to the query runtime numpy file",
    )
    parser.add_argument(
        "--run-warmup-times",
        type=int,
        default=1,
        help="Run the warmup query list this many times.",
    )
    parser.add_argument(
        "--cstr-var",
        type=str,
        help="Set to connect via ODBC instead of the BRAD client (for use with other baselines).",
    )
    parser.add_argument(
        "--query-bank-file", type=str, required=True, help="Path to a query bank."
    )
    parser.add_argument(
        "--query-frequency-path",
        type=str,
        default=None,
        help="path to the frequency to draw each query in query bank",
    )
    parser.add_argument(
        "--num-query-path",
        type=str,
        default=None,
        help="Path to the distribution of number of queries for each period of a day",
    )
    parser.add_argument(
        "--num-client-path",
        type=str,
        default=None,
        help="Path to the distribution of number of clients for each period of a day",
    )
    parser.add_argument("--num-clients", type=int, default=1)
    parser.add_argument("--avg-gap-s", type=float)
    parser.add_argument("--avg-gap-std-s", type=float, default=0.5)
    parser.add_argument(
        "--gap-dist-path",
        type=str,
        default=None,
        help="Path to the distribution regarding the number of concurrent queries",
    )
    parser.add_argument(
        "--time-scale-factor",
        type=int,
        default=100,
        help="trace 1s of simulation as X seconds in real-time to match the num-concurrent-query",
    )
    parser.add_argument("--query-indexes", type=str)
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

    with open(args.query_bank_file, "r", encoding="UTF-8") as file:
        query_bank = [line.strip() for line in file]

    if args.query_frequency_path is not None and os.path.exists(
        args.query_frequency_path
    ):
        query_frequency = np.load(args.query_frequency_path)
        assert len(query_frequency) == len(
            query_bank
        ), "query_frequency size does not match total number of queries"
    else:
        query_frequency = None

    if (
        args.gap_dist_path is not None
        and os.path.exists(args.gap_dist_path)
        and args.time_scale_factor is not None
    ):
        # we can only set the num_concurrent_query trace in presence of time_scale_factor
        execution_gap_dist = np.load(args.gap_dist_path)
    else:
        execution_gap_dist = None

    if (
        args.num_client_path is not None
        and os.path.exists(args.num_client_path)
        and args.time_scale_factor is not None
    ):
        # we can only set the num_concurrent_query trace in presence of time_scale_factor
        with open(args.num_client_path, "rb") as f:
            num_client_trace = pickle.load(f)
    else:
        num_client_trace = None

    if args.query_indexes is None:
        queries = list(range(len(query_bank)))
    else:
        queries = list(map(int, args.query_indexes.split(",")))

    for qidx in queries:
        assert qidx < len(query_bank)
        assert qidx >= 0

    if args.run_warmup:
        run_warmup(args, query_bank, queries)
        return

    mgr = mp.Manager()
    start_queue = [mgr.Queue() for _ in range(args.num_clients)]
    stop_queue = [mgr.Queue() for _ in range(args.num_clients)]

    if args.run_simulation:
        assert (
            args.query_runtime_path is not None
        ), "must provide query runtime to run simulation"
        all_query_runtime = np.load(args.query_runtime_path)
        assert all_query_runtime.shape == (
            len(query_bank),
            3,
        ), "incorrect query runtime file format"
        processes = []
        for idx in range(args.num_clients):
            p = mp.Process(
                target=simulation_runner,
                args=(
                    all_query_runtime,
                    idx,
                    start_queue[idx],
                    stop_queue[idx],
                    args,
                    query_bank,
                    queries,
                    query_frequency,
                    execution_gap_dist,
                    args.wait_for_execute_sim,
                ),
            )
            p.start()
            processes.append(p)
    else:
        processes = []
        for idx in range(args.num_clients):
            p = mp.Process(
                target=runner,
                args=(
                    idx,
                    start_queue[idx],
                    stop_queue[idx],
                    args,
                    query_bank,
                    queries,
                    query_frequency,
                    execution_gap_dist,
                ),
            )
            p.start()
            processes.append(p)

    print("Waiting for startup...", flush=True)
    for i in range(args.num_clients):
        start_queue[i].get()

    global EXECUTE_START_TIME
    EXECUTE_START_TIME = datetime.now().astimezone(pytz.utc)

    if num_client_trace is not None:
        assert args.time_scale_factor is not None, "need to set args.time_scale_factor"
        print("Telling client no.{} to start.".format(0), flush=True)
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
                        "Telling client no.{} to start.".format(add_client), flush=True
                    )
                    stop_queue[add_client].put("")
                    num_running_client += 1
            elif num_running_client > num_client_required:
                # shutting down clients
                for delete_client in range(num_running_client, num_client_required, -1):
                    print(
                        "Telling client no.{} to stop.".format(delete_client - 1),
                        flush=True,
                    )
                    stop_queue[delete_client - 1].put("")
                    num_running_client -= 1
        now = datetime.now().astimezone(pytz.utc)
        total_exec_time_in_s = (now - EXECUTE_START_TIME).total_seconds()
        if finished_one_day:
            print(
                f"Finished executing one day of workload in {total_exec_time_in_s}s, will ignore the rest of "
                f"pre-set execution time {args.run_for_s}s"
            )
        else:
            print(
                f"Executed ended but unable to finish executing the trace of a full day within {args.run_for_s}s"
            )

    else:
        print("Telling all {} clients to start.".format(args.num_clients), flush=True)
        for i in range(args.num_clients):
            stop_queue[i].put("")

    if args.run_for_s and num_client_trace is None:
        print(
            "Waiting for {} seconds...".format(args.run_for_s),
            flush=True,
            file=sys.stderr,
        )
        time.sleep(args.run_for_s)
    elif num_client_trace is None:
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
