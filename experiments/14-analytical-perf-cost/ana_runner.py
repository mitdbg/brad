import asyncio
import argparse
import multiprocessing as mp
import time
import os
import pathlib
import random
import queue
import sys
import signal
import pytz
from typing import List
from datetime import datetime

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.provisioning.directory import Directory
from brad.front_end.engine_connections import EngineConnections
from typing import Dict


def build_query_map(query_bank: str) -> Dict[str, int]:
    queries = []
    with open(query_bank, "r") as file:
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
    engine: Engine,
) -> None:
    def noop(_signal, _frame):
        pass

    signal.signal(signal.SIGINT, noop)

    # For printing out results.
    if "COND_OUT" in os.environ:
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(".")

    config = ConfigFile.load(args.config_file)
    directory = Directory(config)
    asyncio.run(directory.refresh())
    ec = EngineConnections.connect_sync(
        config, directory, "imdb_extended", autocommit=True, specific_engines={engine}
    )
    conn = ec.get_connection(engine)
    cursor = conn.cursor_sync()

    with open(out_dir / "olap_batch_{}.csv".format(runner_idx), "w") as file:
        print("timestamp,query_idx,run_time_s,engine", file=file, flush=True)

        prng = random.Random(args.seed ^ runner_idx)

        # Signal that we're ready to start and wait for the controller.
        start_queue.put_nowait("")
        _ = stop_queue.get()

        while True:
            if args.avg_gap_s is not None:
                wait_for_s = prng.gauss(args.avg_gap_s, args.avg_gap_std_s)
                if wait_for_s < 0.0:
                    wait_for_s = 0.0
                time.sleep(wait_for_s)

            qidx_offset = prng.randint(0, len(queries) - 1)
            qidx = queries[qidx_offset]
            query = query_bank[qidx]

            try:
                engine = None
                now = datetime.now().astimezone(pytz.utc)
                start = time.time()
                cursor.execute_sync(query)
                cursor.fetchall_sync()
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
            except Exception as ex:
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

    ec.close_sync()


def run_warmup(args, query_bank: List[str], queries: List[int], engine: Engine):
    config = ConfigFile.load(args.config_file)
    directory = Directory(config)
    asyncio.run(directory.refresh())
    ec = EngineConnections.connect_sync(
        config, directory, "imdb_extended", autocommit=True, specific_engines={engine}
    )
    conn = ec.get_connection(engine)
    cursor = conn.cursor_sync()

    with open("olap_batch_warmup.csv", "w") as file:
        print("timestamp,query_idx,run_time_s,engine", file=file)
        for idx, qidx in enumerate(queries):
            try:
                engine = None
                query = query_bank[qidx]
                now = datetime.now().astimezone(pytz.utc)
                start = time.time()
                cursor.execute_sync(query)
                cursor.fetchall_sync()
                end = time.time()
                run_time_s = end - start
                print(
                    "Warmed up {} of {}. Run time (s): {}".format(
                        idx + 1, len(queries), run_time_s
                    )
                )
                if run_time_s >= 29:
                    print("Warning: Query index {} takes longer than 30 s".format(idx))
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
            except Exception as ex:
                print(
                    "Unexpected query error:",
                    ex.message(),
                    flush=True,
                    file=sys.stderr,
                )

    ec.close_sync()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run-warmup", action="store_true")
    parser.add_argument(
        "--query_bank_file",
        type=str,
        default="../../workloads/IMDB/OLAP_queries_new/all_queries.sql",
    )
    parser.add_argument("--num-clients", type=int, default=1)
    parser.add_argument("--avg-gap-s", type=float)
    parser.add_argument("--avg-gap-std-s", type=float, default=0.5)
    parser.add_argument("--query-indexes", type=str, required=True)
    parser.add_argument("--config-file", type=str, required=True)
    parser.add_argument("--engine", type=str, required=True)
    parser.add_argument("--run-for-s", type=float, required=True)
    args = parser.parse_args()

    engine = Engine.from_str(args.engine)

    with open(args.query_bank_file, "r", encoding="UTF-8") as file:
        query_bank = [line.strip() for line in file]

    queries = list(map(int, args.query_indexes.split(",")))
    for qidx in queries:
        assert qidx < len(query_bank)
        assert qidx >= 0

    if args.run_warmup:
        run_warmup(args, query_bank, queries, engine)
        return

    mgr = mp.Manager()
    start_queue = mgr.Queue()
    stop_queue = mgr.Queue()

    processes = []
    for idx in range(args.num_clients):
        p = mp.Process(
            target=runner,
            args=(idx, start_queue, stop_queue, args, query_bank, queries, engine),
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
    time.sleep(args.run_for_s)

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
