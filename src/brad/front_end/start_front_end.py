import asyncio
import logging
import signal
import multiprocessing as mp

from brad.config.file import ConfigFile
from brad.front_end.front_end import BradFrontEnd
from brad.utils import set_up_logging

logger = logging.getLogger(__name__)


async def shutdown_server(event_loop):
    logging.debug("Shutting down the event loop...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    event_loop.stop()


def handle_exception(event_loop, context):
    message = context.get("exception", context["message"])
    logging.error("Encountered fatal exception: %s", message)
    logging.error("%s", context)
    if event_loop.is_closed():
        return
    event_loop.create_task(shutdown_server(event_loop))


def legacy_spawn_front_end(args):
    set_up_logging(debug_mode=args.debug)
    config = ConfigFile(args.config_file)

    event_loop = asyncio.new_event_loop()
    event_loop.set_debug(enabled=args.debug)
    asyncio.set_event_loop(event_loop)

    for sig in [signal.SIGTERM, signal.SIGINT]:
        event_loop.add_signal_handler(
            sig, lambda: asyncio.create_task(shutdown_server(event_loop))
        )
    event_loop.set_exception_handler(handle_exception)

    try:
        server = BradFrontEnd(
            config, args.schema_name, args.planner_config_file, args.debug
        )
        event_loop.create_task(server.serve_forever())
        event_loop.run_forever()
    finally:
        event_loop.close()


def start_front_end(
    worker_index: int,
    config_path: str,
    schema_name: str,
    path_to_planner_config: str,
    debug_mode: bool,
    input_queue: mp.Queue,
    output_queue: mp.Queue,
) -> None:
    """
    Schedule this method to run in a child process to launch a BRAD front
    end server.
    """
    config = ConfigFile(config_path)
    set_up_logging(
        filename=config.front_end_log_file(worker_index), debug_mode=debug_mode
    )

    event_loop = asyncio.new_event_loop()
    event_loop.set_debug(enabled=debug_mode)
    asyncio.set_event_loop(event_loop)

    # Signal handlers are inherited from the parent server process. We want
    # to ignore these signals since we receive a shutdown signal from the
    # daemon directly.
    for sig in [signal.SIGTERM, signal.SIGINT]:
        event_loop.add_signal_handler(sig, _noop)

    try:
        front_end = BradFrontEnd(
            worker_index,
            config,
            schema_name,
            path_to_planner_config,
            debug_mode,
            input_queue,
            output_queue,
        )
        event_loop.create_task(front_end.serve_forever())
        logger.info("BRAD front end #%d is starting...", worker_index)
        event_loop.run_forever()
    finally:
        event_loop.close()
        logger.info("BRAD front end #%d has shut down.", worker_index)


def _noop():
    pass
