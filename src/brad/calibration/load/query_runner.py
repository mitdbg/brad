import asyncio
import pathlib
import random
import sys
import time
import queue
import multiprocessing as mp
import io
import signal
from typing import Callable

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.connection.connection import Cursor
from brad.connection.factory import ConnectionFactory
from brad.provisioning.directory import Directory


class Options:
    def __init__(
        self,
        worker_idx: int,
        output_file: pathlib.Path,
        config: ConfigFile,
        engine: Engine,
        schema_name: str,
    ) -> None:
        self.worker_idx = worker_idx
        self.config = config
        self.engine = engine
        self.schema_name = schema_name

        # Set to True if running on Redshift
        self.disable_redshift_cache = False
        self.output_file = output_file

        self.avg_gap_s = 1.0
        self.std_gap_s = 0.5
        self.seed = 42


class Context:
    def __init__(
        self,
        cursor: Cursor,
        output_file: io.TextIOBase,
        prng: random.Random,
        options: Options,
    ) -> None:
        self.cursor = cursor
        self.output_file = output_file
        self.prng = prng
        self.options = options


RunQueryCallable = Callable[[Context], None]


def run_specific_query_until_signalled(
    query_idx: int,
    query: str,
    options: Options,
    start_queue: mp.Queue,
    stop_queue: mp.Queue,
) -> None:
    runner = get_run_specific_query(query_idx, query)
    run_until_signalled(runner, options, start_queue, stop_queue)


def run_until_signalled(
    run_query: RunQueryCallable,
    options: Options,
    start_queue: mp.Queue,
    stop_queue: mp.Queue,
) -> None:
    """
    Meant to be launched as a subprocess with multiprocessing.
    """

    def noop_handler(_signal, _frame):
        pass

    signal.signal(signal.SIGINT, noop_handler)

    directory = Directory(options.config)
    asyncio.run(directory.refresh())
    conn = ConnectionFactory.connect_to_sync(
        options.engine, options.schema_name, options.config, directory
    )
    try:
        cursor = conn.cursor_sync()

        # Hacky way to disable the query cache when applicable.
        if options.disable_redshift_cache:
            print(
                "Disabling Redshift result cache (client {})".format(options.worker_idx)
            )
            cursor.execute_sync("SET enable_result_cache_for_session = OFF;")

        prng = random.Random(options.seed ^ options.worker_idx)

        with open(options.output_file, "w", encoding="UTF-8") as file:
            ctx = Context(cursor, file, prng, options)
            print("query_idx,run_time_s", file=file, flush=True)

            # Signal that we're ready to start and wait for the controller.
            start_queue.put_nowait("")
            _ = stop_queue.get()

            while True:
                run_query(ctx)
                try:
                    _ = stop_queue.get_nowait()
                    break
                except queue.Empty:
                    pass
    finally:
        conn.close_sync()


def get_run_specific_query(query_idx: int, query_str: str) -> RunQueryCallable:
    """
    Runs `query_str` with an optional delay.
    """

    def run_specific_query(ctx: Context) -> None:
        wait_for_s = ctx.prng.gauss(ctx.options.avg_gap_s, ctx.options.std_gap_s)
        if wait_for_s < 0.0:
            wait_for_s = 0.0
        time.sleep(wait_for_s)

        try:
            start = time.time()
            ctx.cursor.execute_sync(query_str)
            ctx.cursor.fetchall_sync()
            end = time.time()
            print(
                "{},{}".format(query_idx, end - start),
                file=ctx.output_file,
                flush=True,
            )

        except Exception as ex:
            print(
                "Skipping query {} because of an error (potentially timeout)".format(
                    query_idx
                ),
                file=sys.stderr,
                flush=True,
            )
            print(ex, file=sys.stderr, flush=True)

    return run_specific_query
