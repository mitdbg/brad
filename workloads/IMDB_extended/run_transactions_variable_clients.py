import asyncio
import argparse
import pathlib
import random
import signal
import time
import numpy as np
import os
import pytz
import multiprocessing as mp
import logging
from datetime import datetime, timedelta
from typing import Optional

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.grpc_client import BradClientError
from brad.provisioning.directory import Directory
from brad.utils.rand_exponential_backoff import RandomizedExponentialBackoff
from brad.utils import set_up_logging, create_custom_logger
from workload_utils.connect import connect_to_db
from workload_utils.transaction_worker import TransactionWorker
from workload_utils.pause_controller import PauseController, get_command_line_input
from workload_utils.change_clients_api import serve

logger = logging.getLogger(__name__)
STARTUP_FAILED = "startup_failed"


def runner(
    args,
    worker_idx: int,
    directory: Optional[Directory],
    start_queue: mp.Queue,
    control_semaphore: mp.Semaphore,  # type: ignore
    pause_semaphore: mp.Semaphore,  # type: ignore
    resume_semaphore: mp.Semaphore,  # type: ignore
) -> None:
    """
    Meant to be launched as a subprocess with multiprocessing.
    """

    def noop_handler(_signal, _frame):
        pass

    signal.signal(signal.SIGINT, noop_handler)

    set_up_logging()

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

    # Connect and set the isolation level.
    try:
        db = connect_to_db(
            args, worker_idx, direct_engine=Engine.Aurora, directory=directory
        )
        db.execute_sync(
            f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {args.isolation_level}"
        )
    except BradClientError as ex:
        logger.error("[T %d] Failed to connect to BRAD: %s", worker_idx, str(ex))
        start_queue.put_nowait(STARTUP_FAILED)
        return

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
        "txn_runner_verbose", str(verbose_log_dir / f"runner_{worker_idx}.log")
    )
    verbose_logger.info("Workload starting...")

    # Signal that we are ready to start and wait for other clients.
    start_queue.put("")
    control_semaphore.acquire()  # type: ignore

    txn_exec_count = 0
    rand_backoff = None
    overall_start = time.time()
    try:
        latency_file = open(
            out_dir / "oltp_latency_{}.csv".format(worker_idx), "w", encoding="UTF-8"
        )
        print("txn_idx,timestamp,run_time_s", file=latency_file)

        pause = False
        while True:
            # Note that `False` means to not block.
            should_exit = control_semaphore.acquire(False)  # type: ignore
            if should_exit:
                logger.info("T Runner %d is exiting.", worker_idx)
                break
            should_pause = pause_semaphore.acquire(False)  # type: ignore
            if should_pause:
                pause = True
            if pause:
                should_resume = resume_semaphore.acquire(False)  # type: ignore
                if should_resume:
                    pause = False
                else:
                    time.sleep(1)
                continue

            txn_exec_count += 1
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
                    logger.info("[T %d] Continued after transient errors.", worker_idx)
                    rand_backoff = None

            except BradClientError as ex:
                succeeded = False
                if ex.is_transient():
                    verbose_logger.warning("Transient txn error: %s", ex.message())

                    if rand_backoff is None:
                        rand_backoff = RandomizedExponentialBackoff(
                            max_retries=100,
                            base_delay_s=0.1,
                            max_delay_s=timedelta(minutes=1).total_seconds(),
                        )
                        logger.info(
                            "[T %d] Backing off due to transient errors.",
                            worker_idx,
                        )

                    # Delay retrying in the case of a transient error (this
                    # happens during blueprint transitions).
                    wait_s = rand_backoff.wait_time_s()
                    if wait_s is None:
                        logger.info(
                            "[T %d] Aborting benchmark. Too many transient errors.",
                            worker_idx,
                        )
                        break
                    verbose_logger.info(
                        "[T %d] Backing off for %.4f seconds...", worker_idx, wait_s
                    )
                    time.sleep(wait_s)

                else:
                    logger.error(
                        "[T %d] Encountered an unexpected `BradClientError`. Aborting the workload...",
                        worker_idx,
                    )
                    raise
            except:
                succeeded = False
                logger.error(
                    "[T %d] Encountered an unexpected error. Aborting the workload...",
                    worker_idx,
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
                if txn_exec_count > 20_000:
                    latency_file.flush()
                    txn_exec_count = 0

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

    finally:
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

        db.close_sync()


def simulation_runner(
    args,
    worker_idx: int,
    start_queue: mp.Queue,
    control_semaphore: mp.Semaphore,  # type: ignore
    pause_semaphore: mp.Semaphore,  # type: ignore
    resume_semaphore: mp.Semaphore,  # type: ignore
) -> None:
    """
    Meant to be launched as a subprocess with multiprocessing.
    """

    def noop_handler(_signal, _frame):
        pass

    signal.signal(signal.SIGINT, noop_handler)

    set_up_logging()

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
    txn_indexes = list(range(len(transactions)))
    commits = [0 for _ in range(len(transactions))]
    aborts = [0 for _ in range(len(transactions))]

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
        "txn_runner_verbose", str(verbose_log_dir / f"runner_{worker_idx}.log")
    )
    verbose_logger.info("Workload starting...")

    # Signal that we are ready to start and wait for other clients.
    start_queue.put("")
    control_semaphore.acquire()  # type: ignore

    txn_exec_count = 0
    rand_backoff = None

    latency_file = open(
        out_dir / "oltp_latency_{}.csv".format(worker_idx), "w", encoding="UTF-8"
    )
    print("txn_idx,timestamp,run_time_s", file=latency_file)

    pause = False
    while True:
        # Note that `False` means to not block.
        should_exit = control_semaphore.acquire(False)  # type: ignore
        if should_exit:
            logger.info("T Runner %d is exiting.", worker_idx)
            break
        should_pause = pause_semaphore.acquire(False)  # type: ignore
        if should_pause:
            pause = True
        if pause:
            should_resume = resume_semaphore.acquire(False)  # type: ignore
            if should_resume:
                pause = False
            else:
                time.sleep(1)
            continue

        txn_exec_count += 1
        txn_idx = txn_prng.choices(txn_indexes, weights=transaction_weights, k=1)[0]

        now = datetime.now().astimezone(pytz.utc)

        succeeded = np.random.choice([True, False], p=[0.8, 0.2])

        if rand_backoff is not None:
            logger.info("[T %d] Continued after transient errors.", worker_idx)
            rand_backoff = None

        time.sleep(0.01)

        # Record metrics.
        if succeeded:
            commits[txn_idx] += 1
        else:
            aborts[txn_idx] += 1

        if txn_prng.random() < args.latency_sample_prob:
            print(
                "{},{},{}".format(txn_idx, now, 0.01),
                file=latency_file,
            )
            if txn_exec_count > 20_000:
                latency_file.flush()
                txn_exec_count = 0

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

    logger.info("[T %d] Done running transactions.", worker_idx)
    latency_file.close()


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
        "--run-simulation",
        action="store_true",
        help="Run the simulation instead of actual execution.",
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
    parser.add_argument("--brad-host", type=str, default="localhost")
    parser.add_argument("--brad-port", type=int, default=6583)
    parser.add_argument("--num-front-ends", type=int, default=1)
    parser.add_argument("--adjust-clients-port", type=int, default=8585)
    parser.add_argument("--interactive", action="store_true")
    parser.add_argument("--starting-clients", type=int)
    args = parser.parse_args()

    set_up_logging()
    logger.info("[T] Preparing to run a steady workload")

    mgr = mp.Manager()
    start_queue = [mgr.Queue() for _ in range(args.num_clients)]
    # pylint: disable-next=no-member
    control_semaphore = [mgr.Semaphore(value=0) for _ in range(args.num_clients)]
    # pylint: disable-next=no-member
    pause_semaphore = [mgr.Semaphore(value=0) for _ in range(args.num_clients)]
    # pylint: disable-next=no-member
    resume_semaphore = [mgr.Semaphore(value=0) for _ in range(args.num_clients)]

    if args.brad_direct:
        assert args.config_file is not None
        assert args.schema_name is not None
        config = ConfigFile.load(args.config_file)
        directory = Directory(config)
        asyncio.run(directory.refresh())
    else:
        directory = None

    clients = []
    if args.run_simulation:
        for idx in range(args.num_clients):
            p = mp.Process(
                target=simulation_runner,
                args=(
                    args,
                    idx,
                    start_queue[idx],
                    control_semaphore[idx],
                    pause_semaphore[idx],
                    resume_semaphore[idx],
                ),
            )
            p.start()
            clients.append(p)
    else:
        for idx in range(args.num_clients):
            p = mp.Process(
                target=runner,
                args=(
                    args,
                    idx,
                    directory,
                    start_queue[idx],
                    control_semaphore[idx],
                    pause_semaphore[idx],
                    resume_semaphore[idx],
                ),
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

    logger.info("[T] Telling all %d clients to start.", args.num_clients)
    for idx in range(args.num_clients):
        control_semaphore[idx].release()

    pause_controller = PauseController(
        args.num_clients, pause_semaphore, resume_semaphore
    )

    if args.starting_clients is not None:
        pause_controller.adjust_num_running_clients(args.starting_clients)

    if args.interactive:
        logger.info(
            "[T] type in an integer smaller than total number of clients and press enter to change number of running client, type in exit to stop dynamically adjusting number of clients...",
        )
        get_command_line_input(pause_controller)
    else:
        logger.info(
            "[T] Listening on %d until requested to stop... (hit Ctrl-C)",
            args.adjust_clients_port,
        )
        serve(pause_controller, port=args.adjust_clients_port)

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
