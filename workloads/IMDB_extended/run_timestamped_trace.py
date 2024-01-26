import pandas as pd
import asyncio
import argparse
import multiprocessing as mp
import math
import time
import os
import pathlib
import signal
import pytz
import logging
import yaml
from typing import List, Optional, Callable, Dict, Any, Tuple
from datetime import datetime
from collections import namedtuple

from workload_utils.connect import connect_to_db, Database
from workload_utils.inflight import InflightHelper
from brad.config.engine import Engine
from brad.grpc_client import BradClientError
from brad.utils import set_up_logging, create_custom_logger

logger = logging.getLogger(__name__)
EXECUTE_START_TIME = datetime.now().astimezone(pytz.utc)
ENGINE_NAMES = ["ATHENA", "AURORA", "REDSHIFT"]
SIMULATION = False

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


def load_trace(
    path_to_manifest: str,
) -> Tuple[Dict[str, List[str]], List[Dict[str, Any]]]:
    manifest_dir = pathlib.Path(path_to_manifest).parent

    with open(path_to_manifest, encoding="UTF-8") as file:
        raw = yaml.load(file, yaml.Loader)

    # Load datasets.
    datasets = {}
    datasets_meta = raw["datasets"]
    for dataset_cfg in datasets_meta:
        with open(manifest_dir / dataset_cfg["path"], "r", encoding="UTF-8") as file:
            query_bank = [line.strip() for line in file]
        datasets[dataset_cfg["name"]] = query_bank

    # Load trace data.
    client_trace = []
    trace_meta = raw["traces"]
    for trace_cfg in trace_meta:
        df = pd.read_csv(manifest_dir / trace_cfg["trace_file"])
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        client_trace.append(
            {
                "raw_trace": df,
                "dataset": trace_cfg["dataset"],
            }
        )

    # Do postprocessing on the trace.
    # Find minimum timestamp.
    starting_ts = None
    for trace in client_trace:
        df = trace["raw_trace"]
        assert isinstance(df, pd.DataFrame)
        this_min = df["timestamp"].min()
        if starting_ts is None:
            starting_ts = this_min
        elif this_min < starting_ts:
            starting_ts = this_min

    assert starting_ts is not None
    logger.info(
        "When processing the trace, the global minimum timestamp is %s",
        str(starting_ts),
    )

    # Compute issue gaps.
    for trace in client_trace:
        df = trace["raw_trace"]
        assert isinstance(df, pd.DataFrame)
        df2 = df.copy()
        df2["g_offset_since_start"] = df2["timestamp"] - starting_ts
        df2["g_offset_since_start_s"] = df2["g_offset_since_start"].dt.total_seconds()
        initial_start = df2["g_offset_since_start_s"].iloc[0]
        df2 = df2.sort_values(by=["g_offset_since_start_s"])
        df2["g_issue_gap_s"] = (
            df2["g_offset_since_start_s"]
            - df2["g_offset_since_start_s"].shift(periods=1)
        ).fillna(initial_start)
        trace["trace"] = df2

    return datasets, client_trace


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
            end = time.time()
            return QueryResult(
                error=None,
                timestamp=timestamp,
                run_time_s=end - start,
                engine=engine.value if engine is not None else None,
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
    datasets: Dict[str, List[str]],
    traces: List[Dict[str, Any]],
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
            datasets,
            traces,
        )
    )


async def runner_impl(
    runner_idx: int,
    start_queue: mp.Queue,
    control_semaphore: mp.Semaphore,  # type: ignore
    args,
    datasets: Dict[str, List[str]],
    traces: List[Dict[str, Any]],
) -> None:
    # For printing out results.
    if "COND_OUT" in os.environ:
        # pylint: disable-next=import-error
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(".")

    verbose_log_dir = out_dir / "verbose_logs"
    verbose_log_dir.mkdir(exist_ok=True)
    verbose_logger = create_custom_logger(
        "trace_runner_verbose", str(verbose_log_dir / f"runner_{runner_idx}.log")
    )
    verbose_logger.info("Workload starting...")

    if args.engine is not None:
        engine = Engine.from_str(args.engine)
    else:
        engine = None

    db_conns: List[Database] = []
    try:
        for slot_idx in range(args.issue_slots):
            if SIMULATION:
                db_conns.append(None)
                continue
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
        logger.error("[Trace %d] Failed to connect to BRAD: %s", runner_idx, str(ex))
        start_queue.put_nowait(STARTUP_FAILED)
        return

    file = open(
        out_dir / "trace_client_{}.csv".format(runner_idx),
        "w",
        encoding="UTF-8",
    )

    try:
        print(
            "timestamp,time_since_execution_s,time_of_day,query_idx,run_time_s,engine",
            file=file,
            flush=True,
        )

        our_trace = traces[runner_idx]
        our_query_bank = datasets[our_trace["dataset"]]
        total_items = len(our_trace["trace"])

        logger.info(
            "[Trace Runner %d] Queries to run: %d. Dataset: %s",
            runner_idx,
            total_items,
            our_trace["dataset"],
        )

        def handle_result(result: QueryResult) -> None:
            try:
                if result.error is not None:
                    ex = result.error
                    logger.error("Unexpected query error: %s", str(ex))
                    return

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
                    "[Trace Runner %d] Unexpected exception when handling query result.",
                    runner_idx,
                )

        inflight_runner = InflightHelper[Database, QueryResult](
            contexts=db_conns, on_result=handle_result
        )

        # Signal that we're ready to start and wait for the controller.
        logger.info("[Trace] Runner %d is ready to start running.", runner_idx)
        start_queue.put_nowait("")
        control_semaphore.acquire()  # type: ignore

        timepoint = time.time()

        for index, row in our_trace["trace"].iterrows():
            # Note that `False` means to not block.
            should_exit = control_semaphore.acquire(False)  # type: ignore
            if should_exit:
                logger.info("[Trace] Runner %d is exiting.", runner_idx)
                break

            qidx = row["query_idx"]
            issue_gap_s = row["g_issue_gap_s"]
            query_sql = our_query_bank[qidx]
            delta = time.time() - timepoint  # Used to adjust for delays.
            issue_gap_s_orig = issue_gap_s
            issue_gap_s -= delta
            verbose_logger.info(
                "Waiting %.2f s (orig: %.2f s) before issuing query index %d",
                issue_gap_s,
                issue_gap_s_orig,
                qidx,
            )
            if not SIMULATION:
                if issue_gap_s > 0.0:
                    await inflight_runner.wait_for_s(issue_gap_s)
            timepoint = time.time()

            now = datetime.now().astimezone(pytz.utc)
            if args.time_scale_factor is not None:
                time_unsimulated = get_time_of_the_day_unsimulated(
                    now, args.time_scale_factor
                )
                time_unsimulated_str = time_in_minute_to_datetime_str(time_unsimulated)
            else:
                time_unsimulated_str = "xxx"

            verbose_logger.info("[Trace %d] Issuing query %d", runner_idx, qidx)
            run_query_fn = get_run_query(
                now,
                qidx,
                query_sql,
                (now - EXECUTE_START_TIME).total_seconds(),
                time_unsimulated_str,
            )
            while True:
                if SIMULATION:
                    break
                was_submitted = inflight_runner.submit(run_query_fn)
                if was_submitted:
                    break
                logger.warning(
                    "[Trace %d] Ran out of issue slots. Waiting for next slot to free up.",
                    runner_idx,
                )
                await inflight_runner.wait_until_next_slot_is_free()

            if index % 100 == 0:
                verbose_logger.info(
                    "[Trace %d] Progress %d / %d", runner_idx, index, total_items
                )

    finally:
        await inflight_runner.wait_until_complete()
        os.fsync(file.fileno())
        file.close()
        if not SIMULATION:
            for db in db_conns:
                db.close_sync()
        logger.info("[Trace] Runner %d has exited.", runner_idx)


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
    parser.add_argument("--client-offset", type=int, default=0)
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
        "--time-scale-factor",
        type=int,
        default=2,
        help="trace 1s of simulation as X seconds in real-time to match the num-concurrent-query",
    )
    parser.add_argument("--issue-slots", type=int, default=10)
    parser.add_argument("--trace-manifest", type=str, required=True)
    args = parser.parse_args()

    set_up_logging()

    logger.info(
        "[Trace Runner] Running with %d issue slots per client.", args.issue_slots
    )
    if args.brad_direct:
        logger.info(
            "[Trace Runner] Running directly against an engine: %s", args.engine
        )
    else:
        logger.info(
            "[Trace Runner] Running on BRAD with %d front ends.", args.num_front_ends
        )

    dataset, client_trace = load_trace(args.trace_manifest)
    for idx, trace in enumerate(client_trace):
        t = trace["trace"]
        logger.info("Client trace %d", idx)
        logger.info("Dataset %s", trace["dataset"])
        logger.info(
            "Trace head:\n%s",
            t[["g_issue_gap_s", "g_offset_since_start_s", "query_idx"]].head(),
        )
        logger.info("Trace length: %d", len(t))
        logger.info("")

    num_clients = len(client_trace)
    logger.info(
        "Will run with %d clients in total, %d slots per client.",
        num_clients,
        args.issue_slots,
    )
    logger.info("Pausing for 10 seconds to allow for aborting.")
    time.sleep(10.0)

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
    start_queue = [mgr.Queue() for _ in range(num_clients)]
    # N.B. `value = 0` since we use this for synchronization, not mutual exclusion.
    # pylint: disable-next=no-member
    control_semaphore = [mgr.Semaphore(value=0) for _ in range(num_clients)]

    processes = []
    for idx in range(num_clients):
        p = mp.Process(
            target=runner,
            args=(
                idx,
                start_queue[idx],
                control_semaphore[idx],
                args,
                dataset,
                client_trace,
            ),
        )
        p.start()
        processes.append(p)

    logger.info("[Trace] Waiting for startup...")
    one_startup_failed = False
    for i in range(num_clients):
        msg = start_queue[i].get()
        if msg == STARTUP_FAILED:
            one_startup_failed = True

    if one_startup_failed:
        logger.error(
            "[Trace] At least one runner failed to start up. Aborting the experiment."
        )
        for i in range(num_clients):
            # Ideally we should be able to release twice atomically.
            control_semaphore[i].release()
            control_semaphore[i].release()
        for p in processes:
            p.join()
        logger.info("[Trace] Overall abort complete.")
        return

    global EXECUTE_START_TIME  # pylint: disable=global-statement
    EXECUTE_START_TIME = datetime.now().astimezone(
        pytz.utc
    )  # pylint: disable=global-statement

    logger.info("[Trace] Telling all %d clients to start.", num_clients)
    for i in range(num_clients):
        control_semaphore[i].release()

    logger.info("[Trace] Waiting for the clients to complete...")
    for p in processes:
        p.join()

    for idx, p in enumerate(processes):
        logger.info("Runner %d exit code: %d", idx, p.exitcode)

    logger.info("Done trace!")


if __name__ == "__main__":
    # On Unix platforms, the default way to start a process is by forking, which
    # is not ideal (we do not want to duplicate this process' file
    # descriptors!).
    mp.set_start_method("spawn")
    main()
