import argparse
import multiprocessing as mp
import time
import os
import pathlib
import random
import queue
import sys
import threading
import signal
from typing import List

from brad.grpc_client import BradGrpcClient
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
) -> None:
    # For printing out results.
    if "COND_OUT" in os.environ:
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(".")

    port_offset = runner_idx % args.num_front_ends

    with BradGrpcClient(args.host, args.port + port_offset) as brad_client, open(
        out_dir / "olap_batch_{}.csv".format(runner_idx), "w"
    ) as file:
        print("query_idx,run_time_s,engine", file=file, flush=True)

        prng = random.Random(args.seed ^ runner_idx)

        # Signal that we're ready to start and wait for the controller.
        start_queue.put_nowait("")
        _ = stop_queue.get()

        while True:
            if args.avg_gap_s is not None:
                wait_for_s = prng.gauss(args.avg_gap_s, 0.5)
                if wait_for_s < 0.0:
                    wait_for_s = 0.0
                time.sleep(wait_for_s)

            qidx_offset = prng.randint(0, len(queries) - 1)
            qidx = queries[qidx_offset]
            query = query_bank[qidx]

            engine = None
            start = time.time()
            _, engine = brad_client.run_query_json(query)
            end = time.time()
            print(
                "{},{},{}".format(
                    qidx,
                    end - start,
                    engine.value if engine is not None else "unknown",
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
    with BradGrpcClient(args.host, args.port) as brad_client, open(
        "olap_batch_warmup.csv", "w"
    ) as file:
        print("query_idx,run_time_s,engine", file=file)
        for idx, qidx in enumerate(queries):
            engine = None
            query = query_bank[qidx]
            start = time.time()
            _, engine = brad_client.run_query_json(query)
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
    parser.add_argument("--num-front-ends", type=int, default=1)
    parser.add_argument("--run-warmup", action="store_true")
    parser.add_argument(
        "--query_bank_file",
        type=str,
        default="../../workloads/IMDB/OLAP_queries_new/all_queries.sql",
    )
    parser.add_argument("--num-clients", type=int, default=1)
    parser.add_argument("--avg-gap-s", type=float)
    parser.add_argument("--query-indexes", type=str, required=True)
    args = parser.parse_args()

    with open(args.query_bank_file, "r", encoding="UTF-8") as file:
        query_bank = [line.strip() for line in file]

    queries = list(map(int, args.query_indexes.split(",")))
    for qidx in queries:
        assert qidx < len(query_bank)
        assert qidx >= 0

    if args.run_warmup:
        run_warmup(args, query_bank, queries)
        return

    mgr = mp.Manager()
    start_queue = mgr.Queue()
    stop_queue = mgr.Queue()

    processes = []
    for idx in range(args.num_clients):
        p = mp.Process(
            target=runner,
            args=(idx, start_queue, stop_queue, args, query_bank, queries),
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
    print(
        "Analytics waiting until requested to stop... (hit Ctrl-C)",
        flush=True,
        file=sys.stderr,
    )
    should_shutdown = threading.Event()

    def signal_handler(_signal, _frame):
        should_shutdown.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    should_shutdown.wait()

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
