import argparse
import multiprocessing as mp
import time
import os
import pathlib
import random
from collections import deque
from datetime import timedelta

from brad.grpc_client import BradGrpcClient
from brad.planner.workload import Workload
from brad.planner.workload.builder import WorkloadBuilder
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
    workload: Workload,
) -> None:
    qidx_map = build_query_map(args.query_bank_file)

    # For printing out results.
    if "COND_OUT" in os.environ:
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(".")

    with BradGrpcClient(args.host, args.port) as brad_client, open(
        out_dir / "olap_batch_{}.csv".format(runner_idx), "w"
    ) as file:
        print("query_idx,run_time_s,engine", file=file, flush=True)

        total_to_run = 0
        queries_to_run = deque()
        for q in workload.analytical_queries():
            total_to_run += q.arrival_count()
            queries_to_run.append((q, q.arrival_count()))

        prng = random.Random(args.seed ^ runner_idx)
        prng.shuffle(queries_to_run)

        # Signal that we're ready to start and wait for the controller.
        start_queue.put_nowait("")
        _ = stop_queue.get()

        idx = 0
        while len(queries_to_run) > 0:
            if idx % 10 == 0:
                print(
                    "Running query index {} of {}".format(idx, total_to_run),
                    flush=True,
                )

            wait_for_s = prng.gauss(1.0, 0.5)
            if wait_for_s < 0.0:
                wait_for_s = 0.0
            time.sleep(wait_for_s)

            idx += 1
            q, count = queries_to_run.popleft()
            qidx = qidx_map[q.raw_query]

            engine = None
            start = time.time()
            for _, engine in brad_client.run_query(q.raw_query):
                # Pull the results from BRAD (row-by-row).
                pass
            end = time.time()
            print(
                "{},{},{}".format(
                    qidx,
                    end - start,
                    engine.value if engine is not None else "unknown",
                ),
                file=file,
            )

            if count > 1:
                queries_to_run.append((q, count - 1))


def run_warmup(args, workload: Workload):
    qidx_map = build_query_map(args.query_bank_file)

    with BradGrpcClient(args.host, args.port) as brad_client, open(
        "olap_batch_warmup.csv", "w"
    ) as file:
        print("query_idx,run_time_s,engine", file=file)
        for idx, q in enumerate(workload.analytical_queries()):
            qidx = qidx_map[q.raw_query]
            engine = None
            start = time.time()
            for _, engine in brad_client.run_query(q.raw_query):
                # Pull the results from BRAD (row-by-row).
                pass
            end = time.time()
            run_time_s = end - start
            print(
                "Warmed up {} of {}. Run time (s): {}".format(
                    idx + 1, len(workload.analytical_queries()), run_time_s
                )
            )
            if run_time_s >= 29:
                print("Warning: Query index {} takes longer than 30 s".format(idx))
            print(
                "{},{},{}".format(
                    qidx, run_time_s, engine.value if engine is not None else "unknown"
                ),
                file=file,
                flush=True,
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=6583)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run_warmup", action="store_true")
    parser.add_argument(
        "--query_bank_file",
        type=str,
        default="../../workloads/IMDB/OLAP_queries/all_queries.sql",
    )
    parser.add_argument("--query_counts_file", type=str)
    parser.add_argument("--ana_num_clients", type=int, default=1)
    args, _ = parser.parse_known_args()

    workload = (
        WorkloadBuilder()
        .add_analytical_queries_and_counts_from_file(
            args.query_bank_file, args.query_counts_file
        )
        .for_period(timedelta(hours=1))
        .build()
    )

    if args.run_warmup:
        run_warmup(args, workload)
        return

    mgr = mp.Manager()
    start_queue = mgr.Queue()
    stop_queue = mgr.Queue()

    processes = []
    for idx in range(args.ana_num_clients):
        p = mp.Process(
            target=runner, args=(idx, start_queue, stop_queue, args, workload)
        )
        p.start()
        processes.append(p)

    print("Waiting for startup...", flush=True)
    for _ in range(args.ana_num_clients):
        start_queue.get()

    print("Telling {} clients to start.".format(args.ana_num_clients), flush=True)
    for _ in range(args.ana_num_clients):
        stop_queue.put("")

    # Wait for the experiment to finish.
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
