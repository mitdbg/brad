import asyncio
import logging
import signal
import multiprocessing as mp

from brad.config.file import ConfigFile
from brad.front_end.front_end import BradFrontEnd
from brad.utils import set_up_logging

logger = logging.getLogger(__name__)


def register_command(subparsers):
    parser = subparsers.add_parser(
        "server",
        help="Start the BRAD server.",
    )
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to BRAD's configuration file.",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        required=True,
        help="The name of the schema to run against.",
    )
    parser.add_argument(
        "--planner-config-file",
        type=str,
        required=True,
        help="Path to the blueprint planner's configuration file.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Set to enable debug logging.",
    )
    parser.set_defaults(func=main)


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


def main(args):
    # On Unix platforms, the default way to start a process is by forking, which
    # is not ideal (we do not want to duplicate this process' file
    # descriptors!).
    mp.set_start_method("spawn")

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
