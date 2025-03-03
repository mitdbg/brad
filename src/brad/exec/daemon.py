import asyncio
import logging
import signal
import multiprocessing as mp

from brad.config.file import ConfigFile
from brad.config.temp_config import TempConfig
from brad.daemon.daemon import BradDaemon
from brad.utils import set_up_logging

logger = logging.getLogger(__name__)


def register_command(subparsers):
    parser = subparsers.add_parser(
        "daemon",
        help="Start the BRAD daemon.",
    )
    parser.add_argument(
        "--physical-config-file",
        type=str,
        required=True,
        help="Path to BRAD's physical configuration file.",
    )
    parser.add_argument(
        "--system-config-file",
        type=str,
        required=True,
        help="Path to BRAD's system configuration file.",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        required=True,
        help="The name of the schema to run against.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Set to enable debug logging.",
    )
    parser.add_argument(
        "--ui", action="store_true", help="Set to enable BRAD's user interface."
    )
    parser.set_defaults(func=main)


async def shutdown_daemon(event_loop):
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
    event_loop.create_task(shutdown_daemon(event_loop))


def drop_into_pdb():
    import pdb

    # N.B. Leaving this in is intentional.
    pdb.set_trace()  # pylint: disable=forgotten-debug-statement


def main(args):
    # On Unix platforms, the default way to start a process is by forking, which
    # is not ideal (we do not want to duplicate this process' file
    # descriptors!).
    mp.set_start_method("spawn")

    config = ConfigFile.load_from_new_configs(
        phys_config=args.physical_config_file, system_config=args.system_config_file
    )
    temp_config = TempConfig.load_from_new_configs(
        system_config=args.system_config_file
    )

    log_path = config.daemon_log_path
    if log_path is not None:
        log_path /= "brad_daemon.log"
    set_up_logging(filename=log_path, debug_mode=args.debug, also_console=True)

    event_loop = asyncio.new_event_loop()
    event_loop.set_debug(enabled=args.debug)
    asyncio.set_event_loop(event_loop)

    for sig in [signal.SIGTERM, signal.SIGINT]:
        event_loop.add_signal_handler(
            sig, lambda: asyncio.create_task(shutdown_daemon(event_loop))
        )
    # This is useful for debugging purposes.
    event_loop.add_signal_handler(signal.SIGUSR1, drop_into_pdb)
    event_loop.set_exception_handler(handle_exception)

    try:
        daemon = BradDaemon(
            config,
            temp_config,
            args.schema_name,
            args.system_config_file,
            args.debug,
            args.ui,
        )
        event_loop.create_task(daemon.run_forever())
        event_loop.run_forever()
    finally:
        event_loop.close()
