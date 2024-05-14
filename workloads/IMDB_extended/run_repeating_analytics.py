import asyncio
import argparse
import copy
import multiprocessing as mp
import math
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
from typing import List, Optional, Callable
import numpy.typing as npt
from datetime import datetime, timedelta
from collections import namedtuple

from workload_utils.backoff_helper import BackoffHelper
from workload_utils.connect import connect_to_db, Database
from workload_utils.inflight import InflightHelper
from brad.config.engine import Engine
from brad.grpc_client import BradClientError
from brad.utils.rand_exponential_backoff import RandomizedExponentialBackoff
from brad.utils import set_up_logging, create_custom_logger

logger = logging.getLogger(__name__)
EXECUTE_START_TIME = datetime.now().astimezone(pytz.utc)
ENGINE_NAMES = ["ATHENA", "AURORA", "REDSHIFT"]

STARTUP_FAILED = "startup_failed"

QueryResult = namedtuple(
    "QueryResult",
    [
        "error",
        "timestamp",
        "run_time_s",
        "engine",
        "query_idx",
        "time_since_execution_s",
        "time_of_day",
    ],
)


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


def get_run_query(
    timestamp: datetime,
    query_idx: int,
    query: str,
    time_since_execution: float,
    time_of_day: str,
) -> Callable[[Database], QueryResult]:
    def _run_query(db: Database) -> QueryResult:
        try:
            start = time.time()
            _, engine = db.execute_sync_with_engine(query)
            if engine is None:
                engine_name = "tidb"
            elif isinstance(engine, Engine):
                engine_name = engine.value
            else:
                engine_name = engine
            end = time.time()
            return QueryResult(
                error=None,
                timestamp=timestamp,
                run_time_s=end - start,
                engine=engine_name,
                query_idx=query_idx,
                time_since_execution_s=time_since_execution,
                time_of_day=time_of_day,
            )
        except Exception as ex:
            return QueryResult(
                error=ex,
                timestamp=timestamp,
                run_time_s=math.nan,
                engine=None,
                query_idx=query_idx,
                time_since_execution_s=time_since_execution,
                time_of_day=time_of_day,
            )

    return _run_query


def runner(
    runner_idx: int,
    start_queue: mp.Queue,
    control_semaphore: mp.Semaphore,  # type: ignore
    args,
    query_bank: List[str],
    queries: List[int],
    query_frequency: Optional[npt.NDArray] = None,
    execution_gap_dist: Optional[npt.NDArray] = None,
) -> None:
    def noop(_signal, _frame):
        pass

    signal.signal(signal.SIGINT, noop)

    set_up_logging()
    asyncio.run(
        runner_impl(
            runner_idx,
            start_queue,
            control_semaphore,
            args,
            query_bank,
            queries,
            query_frequency,
            execution_gap_dist,
        )
    )


async def runner_impl(
    runner_idx: int,
    start_queue: mp.Queue,
    control_semaphore: mp.Semaphore,  # type: ignore
    args,
    query_bank: List[str],
    queries: List[int],
    query_frequency: Optional[npt.NDArray] = None,
    execution_gap_dist: Optional[npt.NDArray] = None,
) -> None:
    # For printing out results.
    if "COND_OUT" in os.environ:
        # pylint: disable-next=import-error
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(f"./{args.output_dir}").resolve()

    verbose_log_dir = out_dir / "verbose_logs"
    verbose_log_dir.mkdir(exist_ok=True)
    verbose_logger = create_custom_logger(
        "repeating_analytics_verbose", str(verbose_log_dir / f"runner_{runner_idx}.log")
    )
    verbose_logger.info("Workload starting...")

    if args.engine is not None:
        engine = Engine.from_str(args.engine)
    else:
        engine = None

    db_conns: List[Database] = []
    try:
        for slot_idx in range(args.issue_slots):
            db_conns.append(
                connect_to_db(
                    args,
                    # Ensures connections are distributed across the front ends.
                    runner_idx + slot_idx,
                    direct_engine=engine,
                    # Ensure we disable the result cache if we are running directly on
                    # Redshift.
                    disable_direct_redshift_result_cache=True,
                    verbose_logger=verbose_logger,
                )
            )
    except BradClientError as ex:
        logger.error("[RA %d] Failed to connect to BRAD: %s", runner_idx, str(ex))
        start_queue.put_nowait(STARTUP_FAILED)
        return

    if query_frequency is not None:
        # There are no predictions for query 48 in our test set (query cannot be parsed).
        # Set its frequency to 0 so it is never used.
        query_frequency[48] = 0.0

        query_frequency = query_frequency[queries]
        query_frequency = query_frequency / np.sum(query_frequency)

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
        bh = BackoffHelper()

        logger.info(
            "[Repeating Analytics Runner %d] Queries to run: %s",
            runner_idx,
            queries,
        )
        query_order_main = queries.copy()
        prng.shuffle(query_order_main)
        query_order = query_order_main.copy()

        first_run = True

        def handle_result(result: QueryResult) -> None:
            try:
                if result.error is not None:
                    ex = result.error
                    if ex.is_transient():
                        verbose_logger.warning(
                            "Transient query error: %s", ex.message()
                        )

                        if bh.backoff is None:
                            bh.backoff = RandomizedExponentialBackoff(
                                max_retries=100,
                                base_delay_s=1.0,
                                max_delay_s=timedelta(minutes=1).total_seconds(),
                            )
                            bh.backoff_timestamp = datetime.now().astimezone(pytz.utc)
                            logger.info(
                                "[RA %d] Backing off due to transient errors.",
                                runner_idx,
                            )
                    else:
                        logger.error("Unexpected query error: %s", ex.message())
                    return

                if bh.backoff is not None and bh.backoff_timestamp is not None:
                    if bh.backoff_timestamp < result.timestamp:
                        # We recovered. This means a query issued after the rand
                        # backoff was created finished successfully.
                        bh.backoff = None
                        bh.backoff_timestamp = None
                        logger.info(
                            "[RA %d] Continued after transient errors.", runner_idx
                        )

                # Record execution result.
                print(
                    "{},{},{},{},{},{}".format(
                        result.timestamp,
                        result.time_since_execution_s,
                        result.time_of_day,
                        result.query_idx,
                        result.run_time_s,
                        result.engine,
                    ),
                    file=file,
                    flush=True,
                )
            except:  # pylint: disable=bare-except
                logger.exception(
                    "[RA %d] Unexpected exception when handling query result.",
                    runner_idx,
                )

        inflight_runner = InflightHelper[Database, QueryResult](
            contexts=db_conns, on_result=handle_result
        )

        # Signal that we're ready to start and wait for the controller.
        logger.info("[RA] Runner %d is ready to start running.", runner_idx)
        start_queue.put_nowait("")
        control_semaphore.acquire()  # type: ignore

        while True:
            # Note that `False` means to not block.
            should_exit = control_semaphore.acquire(False)  # type: ignore
            if should_exit:
                logger.info("[RA] Runner %d is exiting.", runner_idx)
                break

            if bh.backoff is not None:
                # Delay retrying in the case of a transient error (this
                # happens during blueprint transitions).
                wait_s = bh.backoff.wait_time_s()
                if wait_s is None:
                    logger.error(
                        "[RA %d] Aborting benchmark. Too many transient errors.",
                        runner_idx,
                    )
                    break
                verbose_logger.info(
                    "[RA %d] Backing off for %.4f seconds...", runner_idx, wait_s
                )
                await inflight_runner.wait_for_s(wait_s)

            if execution_gap_dist is not None:
                now = datetime.now().astimezone(pytz.utc)
                time_unsimulated = get_time_of_the_day_unsimulated(
                    now, args.time_scale_factor
                )
                wait_for_s = execution_gap_dist[
                    int(time_unsimulated / (60 * 24) * len(execution_gap_dist))
                ]
                await inflight_runner.wait_for_s(wait_for_s)

            elif args.avg_gap_s is not None:
                if first_run:
                    # We wait a uniformly random amount of time at the beginning
                    # to stagger queries across all the clients that will run
                    # (e.g., to avoid having all clients issue queries at the
                    # same time).
                    first_run = False
                    wait_for_s = prng.uniform(0.0, args.avg_gap_s)
                    await inflight_runner.wait_for_s(wait_for_s)
                else:
                    # Wait times are normally distributed if execution_gap_dist is not provided.
                    wait_for_s = prng.gauss(args.avg_gap_s, args.avg_gap_std_s)
                    if wait_for_s < 0.0:
                        wait_for_s = 0.0
                    await inflight_runner.wait_for_s(wait_for_s)

            if query_frequency is not None:
                qidx = prng.choices(queries, list(query_frequency))[0]
            else:
                if len(query_order) == 0:
                    query_order = query_order_main.copy()

                qidx = query_order.pop()
            logger.debug("Executing qidx: %d", qidx)
            query = query_bank[qidx]

            now = datetime.now().astimezone(pytz.utc)
            if args.time_scale_factor is not None:
                time_unsimulated = get_time_of_the_day_unsimulated(
                    now, args.time_scale_factor
                )
                time_unsimulated_str = time_in_minute_to_datetime_str(time_unsimulated)
            else:
                time_unsimulated_str = "xxx"

            verbose_logger.info("[RA %d] Issuing query %d", runner_idx, qidx)
            run_query_fn = get_run_query(
                now,
                qidx,
                query,
                (now - EXECUTE_START_TIME).total_seconds(),
                time_unsimulated_str,
            )
            while True:
                was_submitted = inflight_runner.submit(run_query_fn)
                if was_submitted:
                    break
                verbose_logger.info(
                    "[RA %d] Ran out of issue slots. Waiting for next slot to free up.",
                    runner_idx,
                )
                await inflight_runner.wait_until_next_slot_is_free()

    finally:
        await inflight_runner.wait_until_complete()
        os.fsync(file.fileno())
        file.close()
        for db in db_conns:
            db.close_sync()
        logger.info("[RA] Runner %d has exited.", runner_idx)


def simulation_runner(
    all_query_runtime: npt.NDArray,
    runner_idx: int,
    start_queue: mp.Queue,
    control_semaphore: mp.Semaphore,  # type: ignore
    args,
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
        out_dir = pathlib.Path(f"./{args.output_dir}").resolve()

    if query_frequency_original is not None:
        query_frequency = copy.deepcopy(query_frequency_original)
        # There are no predictions for query 48 in our test set (query cannot be parsed).
        # Set its frequency to 0 so it is never used.
        query_frequency[48] = 0.0
        query_frequency = query_frequency[queries]
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
        control_semaphore.acquire()  # type: ignore

        while True:
            should_exit = control_semaphore.acquire(False)  # type: ignore
            if should_exit:
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
                    query_order = queries.copy()
                    prng.shuffle(query_order)

                qidx = query_order.pop()
            logger.debug("Executing qidx: %d", qidx)
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
        out_dir = pathlib.Path(f"./{args.output_dir}").resolve()

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
        "--num-client-path",
        type=str,
        default=None,
        help="Path to the distribution of number of clients for each period of a day",
    )
    parser.add_argument("--num-clients", type=int, default=1)
    parser.add_argument("--client-offset", type=int, default=0)
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
        default=50,
        help="trace 1s of simulation as X seconds in real-time to match the num-concurrent-query",
    )
    parser.add_argument(
        "--num-client-multiplier",
        type=int,
        default=1,
        help="The multiplier to the number of clients for each period of a day",
    )
    parser.add_argument("--query-indexes", type=str)
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
    parser.add_argument(
        "--ff-trace-clients",
        type=int,
        help="Start the client trace at the given number of clients. Used for debugging only.",
    )
    parser.add_argument("--issue-slots", type=int, default=1)
    args = parser.parse_args()

    set_up_logging()

    logger.info("[RA] Running with %d issue slots per client.", args.issue_slots)

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

    if args.query_indexes is None or args.query_indexes == "":
        queries = list(range(len(query_bank)))
    else:
        queries = list(map(int, args.query_indexes.split(",")))

    for qidx in queries:
        assert qidx < len(query_bank)
        assert qidx >= 0

    if args.run_warmup:
        run_warmup(args, query_bank, queries)
        return

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
                    control_semaphore[idx],
                    args,
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
                    control_semaphore[idx],
                    args,
                    query_bank,
                    queries,
                    query_frequency,
                    execution_gap_dist,
                ),
            )
            p.start()
            processes.append(p)

    logger.info("[RA] Waiting for startup...")
    one_startup_failed = False
    for i in range(args.num_clients):
        msg = start_queue[i].get()
        if msg == STARTUP_FAILED:
            one_startup_failed = True

    if one_startup_failed:
        logger.error(
            "[RA] At least one runner failed to start up. Aborting the experiment."
        )
        for i in range(args.num_clients):
            # Ideally we should be able to release twice atomically.
            control_semaphore[i].release()
            control_semaphore[i].release()
        for p in processes:
            p.join()
        logger.info("[RA] Overall abort complete.")
        return

    global EXECUTE_START_TIME  # pylint: disable=global-statement
    EXECUTE_START_TIME = datetime.now().astimezone(
        pytz.utc
    )  # pylint: disable=global-statement

    if num_client_trace is not None:
        logger.info("[RA] Scaling number of clients by %d", args.num_client_multiplier)
        for k in num_client_trace.keys():
            num_client_trace[k] *= args.num_client_multiplier

        assert args.time_scale_factor is not None, "Need to set --time-scale-factor"
        assert args.run_for_s is not None, "Need to set --run-for-s"

        num_running_client = 0
        num_client_required = min(num_client_trace[0], args.num_clients)
        for add_client in range(num_running_client, num_client_required):
            logger.info("[RA] Telling client no. %d to start.", add_client)
            control_semaphore[add_client].release()
            num_running_client += 1

        finished_one_day = True
        curr_day_start_time = datetime.now().astimezone(pytz.utc)

        fast_forward_tod = None
        if args.ff_trace_clients is not None:
            for tod, clients in num_client_trace.items():
                if clients >= args.ff_trace_clients:
                    fast_forward_tod = tod
                    break
            if fast_forward_tod is not None:
                curr_day_start_time -= timedelta(
                    seconds=fast_forward_tod / args.time_scale_factor
                )
                logger.info(
                    "Fast forward set. Will start the trace at unscaled time of day %d",
                    fast_forward_tod,
                )

        for time_of_day, num_expected_clients in num_client_trace.items():
            if time_of_day == 0:
                continue
            if fast_forward_tod is not None and time_of_day < fast_forward_tod:
                # Used for debugging.
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
                    print(f"[Analytics] Waiting for {wait_time}s")
                    time.sleep(wait_time)
                finished_one_day = False
                break
            print(f"[Analytics] Waiting for {time_in_s-curr_time_in_s}s")
            time.sleep(time_in_s - curr_time_in_s)
            num_client_required = min(num_expected_clients, args.num_clients)
            if num_client_required > num_running_client:
                # starting additional clients
                for add_client in range(num_running_client, num_client_required):
                    logger.info("[RA] Telling client no. %d to start.", add_client)
                    control_semaphore[add_client].release()
                    num_running_client += 1
            elif num_running_client > num_client_required:
                # shutting down clients
                for delete_client in range(num_running_client, num_client_required, -1):
                    logger.info(
                        "[RA] Telling client no. %d to stop.", (delete_client - 1)
                    )
                    control_semaphore[delete_client - 1].release()
                    num_running_client -= 1
        now = datetime.now().astimezone(pytz.utc)
        total_exec_time_in_s = (now - EXECUTE_START_TIME).total_seconds()
        if finished_one_day:
            logger.info(
                "[RA] Finished executing one day of workload in %d s, will ignore the rest of "
                "pre-set execution time %d s",
                total_exec_time_in_s,
                args.run_for_s,
            )
        else:
            logger.info(
                "[RA] Executed ended but unable to finish executing the trace of a full day within %d s",
                args.run_for_s,
            )

    else:
        logger.info("[RA] Telling all %d clients to start.", args.num_clients)
        for i in range(args.num_clients):
            control_semaphore[i].release()

    if args.run_for_s is not None and num_client_trace is None:
        logger.info("[RA] Waiting for %d seconds...", args.run_for_s)
        time.sleep(args.run_for_s)
    elif num_client_trace is None:
        # Wait until requested to stop.
        logger.info(
            "Repeating analytics waiting until requested to stop... (hit Ctrl-C)",
        )
        should_shutdown = threading.Event()

        def signal_handler(_signal, _frame):
            should_shutdown.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        should_shutdown.wait()

    logger.info("[RA] Stopping all clients...")
    for i in range(args.num_clients):
        # Note that in most cases, one release will have already run. This is OK
        # because downstream runners will not hang if there is a unconsumed
        # semaphore value.
        control_semaphore[i].release()
        control_semaphore[i].release()

    logger.info("[RA] Waiting for the clients to complete...")
    for p in processes:
        p.join()

    for idx, p in enumerate(processes):
        logger.info("Runner %d exit code: %d", idx, p.exitcode)

    logger.info("Done repeating analytics!")


if __name__ == "__main__":
    # On Unix platforms, the default way to start a process is by forking, which
    # is not ideal (we do not want to duplicate this process' file
    # descriptors!).
    mp.set_start_method("spawn")
    main()
