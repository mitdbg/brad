import asyncio
import math
import argparse
import pathlib
import pickle
import random
import signal
import threading
import time
import os
import pickle
import pytz
import multiprocessing as mp
import logging
from collections import namedtuple
from datetime import datetime, timedelta
from typing import Optional, Callable

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.grpc_client import BradClientError
from brad.provisioning.directory import Directory
from brad.utils.rand_exponential_backoff import RandomizedExponentialBackoff
from brad.utils.time_periods import universal_now
from brad.utils import set_up_logging, create_custom_logger
from workload_utils.connect import connect_to_db, Database
from workload_utils.transaction_worker import TransactionWorker
from workload_utils.backoff_helper import BackoffHelper
from workload_utils.inflight import InflightHelper

logger = logging.getLogger(__name__)
STARTUP_FAILED = "startup_failed"

TxnResult = namedtuple("TxnResult", ["error", "txn_idx", "timestamp", "run_time_s"])


def runner(
    args,
    worker_idx: int,
    directory: Optional[Directory],
    start_queue: mp.Queue,
    control_semaphore: mp.Semaphore,  # type: ignore
) -> None:
    """
    Meant to be launched as a subprocess with multiprocessing.
    """

    def noop_handler(_signal, _frame):
        pass

    signal.signal(signal.SIGINT, noop_handler)

    set_up_logging()
    asyncio.run(
        runner_impl(args, worker_idx, directory, start_queue, control_semaphore)
    )


async def runner_impl(
    args,
    worker_idx: int,
    directory: Optional[Directory],
    start_queue: mp.Queue,
    control_semaphore: mp.Semaphore,  # type: ignore
) -> None:
    worker = TransactionWorker(
        worker_idx,
        args.seed ^ worker_idx,
        args.scale_factor,
        args.dataset_type,
        args.use_zipfian_ids,
        args.zipfian_alpha,
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
    db_conns = []

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
        "txn_runner_verbose", str(verbose_log_dir / f"runner_{worker_idx}.log")
    )

    # Connect and set the isolation level.
    try:
        for slot_idx in range(args.issue_slots):
            db = connect_to_db(
                args,
                worker_idx + slot_idx,
                direct_engine=Engine.Aurora,
                directory=directory,
                verbose_logger=verbose_logger,
            )
            if args.baseline != "tidb":
                db.execute_sync(
                    f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {args.isolation_level}"
                )
            db_conns.append(db)
    except BradClientError as ex:
        logger.error("[T %d] Failed to connect to BRAD: %s", worker_idx, str(ex))
        start_queue.put_nowait(STARTUP_FAILED)
        return

    verbose_logger.info("Workload starting...")

    bh = BackoffHelper()

    def get_run_txn(txn_idx: int) -> Callable[[Database], TxnResult]:
        txn = transactions[txn_idx]
        # pylint: disable-next=comparison-with-callable
        if txn == worker.purchase_tickets:
            select_using_name = txn_prng.random() < lookup_theatre_id_by_name
        else:
            select_using_name = False

        def _run_txn(db: Database) -> TxnResult:
            try:
                start_ts = datetime.now().astimezone(pytz.utc)
                start = time.time()
                # pylint: disable-next=comparison-with-callable
                if txn == worker.purchase_tickets:
                    txn(db, select_using_name=select_using_name)
                else:
                    txn(db)
                end = time.time()
                return TxnResult(
                    error=None,
                    txn_idx=txn_idx,
                    timestamp=start_ts,
                    run_time_s=end - start,
                )
            except Exception as ex:
                return TxnResult(
                    error=ex,
                    txn_idx=txn_idx,
                    timestamp=start_ts,
                    run_time_s=math.nan,
                )

        return _run_txn

    def handle_result(result: TxnResult) -> None:
        try:
            # Record metrics.
            bh.counter += 1
            if result.error is None:
                commits[txn_idx] += 1
            else:
                aborts[txn_idx] += 1

            if result.error is not None:
                ex = result.error
                if ex.is_transient():
                    verbose_logger.warning("Transient txn error: %s", ex.message())

                    if bh.backoff is None:
                        bh.backoff = RandomizedExponentialBackoff(
                            max_retries=100,
                            base_delay_s=0.1,
                            max_delay_s=timedelta(minutes=1).total_seconds(),
                        )
                        bh.backoff_timestamp = datetime.now().astimezone(pytz.utc)
                        logger.info(
                            "[T %d] Backing off due to transient errors.",
                            worker_idx,
                        )

                else:
                    logger.error(
                        "[T %d] Encountered an unexpected `BradClientError`: %s",
                        worker_idx,
                        str(ex),
                    )
                return

            if txn_prng.random() < args.latency_sample_prob:
                print(
                    "{},{},{}".format(
                        result.txn_idx, result.timestamp, result.run_time_s
                    ),
                    file=latency_file,
                )
                if bh.counter > 10_000:
                    latency_file.flush()
                    bh.counter = 0

                # Warn if the abort rate is high.
                total_aborts = sum(aborts)
                total_commits = sum(commits)
                abort_rate = total_aborts / (total_aborts + total_commits)
                if abort_rate > 0.15:
                    logger.info(
                        "[T %d] Abort rate is higher than expected ({%4f}).",
                        worker_idx,
                        abort_rate,
                    )
        except:  # pylint: disable=bare-except
            logger.exception(
                "[T %d] Unexpected error when handling transaction result.", worker_idx
            )

    inflight_runner = InflightHelper[Database, TxnResult](
        contexts=db_conns, on_result=handle_result
    )
    first_run = True

    logger.info("[T %d] Ready to start.", worker_idx)

    # Signal that we are ready to start and wait for other clients.
    start_queue.put("")
    control_semaphore.acquire()  # type: ignore

    overall_start = time.time()
    try:
        latency_file = open(
            out_dir / "oltp_latency_{}.csv".format(worker_idx), "w", encoding="UTF-8"
        )
        print("txn_idx,timestamp,run_time_s", file=latency_file, flush=True)

        while True:
            # Note that `False` means to not block.
            should_exit = control_semaphore.acquire(False)  # type: ignore
            if should_exit:
                logger.info("T Runner %d is exiting.", worker_idx)
                break

            if bh.backoff is not None:
                # Delay retrying in the case of a transient error (this
                # happens during blueprint transitions).
                wait_s = bh.backoff.wait_time_s()
                if wait_s is None:
                    logger.info(
                        "[T %d] Aborting benchmark. Too many transient errors.",
                        worker_idx,
                    )
                    break
                verbose_logger.info(
                    "[T %d] Backing off for %.4f seconds...", worker_idx, wait_s
                )
                await inflight_runner.wait_for_s(wait_s)

            elif args.avg_gap_s is not None:
                if first_run:
                    # We wait a uniformly random amount of time at the beginning
                    # to stagger queries across all the clients that will run
                    # (e.g., to avoid having all clients issue transactions at
                    # the same time).
                    first_run = False
                    wait_for_s = txn_prng.uniform(0.0, args.avg_gap_s)
                    await inflight_runner.wait_for_s(wait_for_s)
                else:
                    # Wait times are normally distributed.
                    wait_for_s = txn_prng.gauss(args.avg_gap_s, args.avg_gap_std_s)
                    if wait_for_s < 0.0:
                        wait_for_s = 0.0
                    await inflight_runner.wait_for_s(wait_for_s)

            txn_idx = txn_prng.choices(txn_indexes, weights=transaction_weights, k=1)[0]
            txn_fn = get_run_txn(txn_idx)
            while True:
                submitted = inflight_runner.submit(txn_fn)
                if submitted:
                    break
                verbose_logger.info(
                    "[T %d] Ran out of issue slots. Waiting for next slot to free up.",
                    worker_idx,
                )
                await inflight_runner.wait_until_next_slot_is_free()

    finally:
        await inflight_runner.wait_until_complete()
        overall_end = time.time()
        logger.info("[T %d] Done running transactions.", worker_idx)
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

        for db in db_conns:
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
        "--dataset-type",
        choices=["original", "20gb", "100gb"],
        default="original",
        help="This controls the range of reads the transaction worker performs, "
        "depending on the dataset size.",
    )
    parser.add_argument(
        "--use-zipfian-ids",
        action="store_true",
        help="Whether the transaction worker should draw movie and theatre IDs "
        "from a Zipfian distribution.",
    )
    parser.add_argument(
        "--zipfian-alpha",
        type=float,
        default=1.1,
        help="The alpha parameter for the Zipfian distribution. Only used if "
        "--use-zipfian-ids is `True`. Must be strictly greater than 1. ",
    )
    # These three arguments are used for the day long experiment.
    parser.add_argument(
        "--num-client-path",
        type=str,
        default=None,
        help="Path to the distribution of number of clients for each period of a day",
    )
    parser.add_argument(
        "--num-client-multiplier",
        type=int,
        default=1,
        help="The multiplier to the number of clients for each period of a day",
    )
    parser.add_argument(
        "--time-scale-factor",
        type=int,
        default=100,
        help="trace 1s of simulation as X seconds in real-time to match the num-concurrent-query",
    )
    parser.add_argument("--brad-host", type=str, default="localhost")
    parser.add_argument("--brad-port", type=int, default=6583)
    parser.add_argument("--num-front-ends", type=int, default=1)
    parser.add_argument("--issue-slots", type=int, default=1)
    # 25 milliseconds.
    parser.add_argument("--avg-gap-s", type=float, default=0.025)
    # 2 milliseconds.
    parser.add_argument("--avg-gap-std-s", type=float, default=0.002)
    args = parser.parse_args()

    set_up_logging()
    logger.info("[T] Running with %d issue slots per client.", args.issue_slots)

    if (
        args.num_client_path is not None
        and os.path.exists(args.num_client_path)
        and args.time_scale_factor is not None
    ):
        # we can only set the num_concurrent_query trace in presence of time_scale_factor
        with open(args.num_client_path, "rb") as f:
            num_client_trace = pickle.load(f)
        logger.info("[T] Preparing to run a time varying workload")
    else:
        num_client_trace = None
        logger.info("[T] Preparing to run a steady workload")


    mgr = mp.Manager()
    start_queue = [mgr.Queue() for _ in range(args.num_clients)]
    # pylint: disable-next=no-member
    control_semaphore = [mgr.Semaphore(value=0) for _ in range(args.num_clients)]


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
            target=runner,
            args=(args, idx, directory, start_queue[idx], control_semaphore[idx]),
        )
        p.start()
        clients.append(p)

    logger.info("[T] Waiting for startup...")
    one_startup_failed = False
    for i in range(args.num_clients):
        msg = start_queue[i].get()
        if msg == STARTUP_FAILED:
            one_startup_failed = True

    if one_startup_failed:
        logger.error(
            "At least one transactional runner failed to start up. Aborting the experiment.",
        )
        for i in range(args.num_clients):
            control_semaphore[i].release()
            control_semaphore[i].release()
        for p in clients:
            p.join()
        logger.info("Transactional client abort complete.")
        return

    if num_client_trace is not None:
        logger.info("[T] Scaling number of clients by %d", args.num_client_multiplier)
        for k in num_client_trace.keys():
            num_client_trace[k] *= args.num_client_multiplier

        assert args.time_scale_factor is not None, "Need to set --time-scale-factor"
        assert args.run_for_s is not None, "Need to set --run-for-s"

        execute_start_time = universal_now()
        num_running_client = 0
        num_client_required = min(num_client_trace[0], args.num_clients)
        for add_client in range(num_running_client, num_client_required):
            logger.info("[T] Telling client no. %d to start.", add_client)
            control_semaphore[add_client].release()
            num_running_client += 1

        finished_one_day = True
        curr_day_start_time = datetime.now().astimezone(pytz.utc)
        for time_of_day, num_expected_clients in num_client_trace.items():
            if time_of_day == 0:
                continue
            # at this time_of_day start/shut-down more clients
            time_in_s = time_of_day / args.time_scale_factor
            now = datetime.now().astimezone(pytz.utc)
            curr_time_in_s = (now - curr_day_start_time).total_seconds()
            total_exec_time_in_s = (now - execute_start_time).total_seconds()
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
            num_client_required = min(num_expected_clients, args.num_clients)
            if num_client_required > num_running_client:
                # starting additional clients
                for add_client in range(num_running_client, num_client_required):
                    logger.info("[T] Telling client no. %d to start.", add_client)
                    control_semaphore[add_client].release()
                    num_running_client += 1
            elif num_running_client > num_client_required:
                # shutting down clients
                for delete_client in range(num_running_client, num_client_required, -1):
                    logger.info(
                        "[T] Telling client no. %d to stop.", (delete_client - 1)
                    )
                    control_semaphore[delete_client - 1].release()
                    num_running_client -= 1
        now = datetime.now().astimezone(pytz.utc)
        total_exec_time_in_s = (now - execute_start_time).total_seconds()
        if finished_one_day:
            logger.info(
                "[T] Finished executing one day of workload in %d s, will ignore the rest of "
                "pre-set execution time %d s",
                total_exec_time_in_s,
                args.run_for_s,
            )
        else:
            logger.info(
                "[T] Executed ended but unable to finish executing the trace of a full day within %d s",
                args.run_for_s,
            )

    else:
        logger.info("[T] Telling all %d clients to start.", args.num_clients)
        for idx in range(args.num_clients):
            control_semaphore[idx].release()

    if args.run_for_s is not None and num_client_trace is None:
        logger.info("[T] Letting the experiment run for %d seconds...", args.run_for_s)
        time.sleep(args.run_for_s)

    elif num_client_trace is None:
        logger.info("[T] Waiting until requested to stop... (hit Ctrl-C)")
        should_shutdown = threading.Event()

        def signal_handler(_signal, _frame):
            should_shutdown.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        should_shutdown.wait()

    logger.info("[T] Stopping clients...")
    for idx in range(args.num_clients):
        # Note that in most cases, one release will have already run. This is OK
        # because downstream runners will not hang if there is a unconsumed
        # semaphore value.
        control_semaphore[idx].release()
        control_semaphore[idx].release()

    logger.info("[T] Waiting for clients to terminate...")
    for c in clients:
        c.join()
    logger.info("[T] Done transactions!")


if __name__ == "__main__":
    # On Unix platforms, the default way to start a process is by forking, which
    # is not ideal (we do not want to duplicate this process' file
    # descriptors!).
    mp.set_start_method("spawn")
    main()
