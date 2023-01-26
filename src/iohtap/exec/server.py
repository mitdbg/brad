import logging
import signal
import threading

from iohtap.config.file import ConfigFile
from iohtap.server.server import IOHTAPServer
from iohtap.utils import set_up_logging


def register_command(subparsers):
    parser = subparsers.add_parser(
        "server",
        help="Start the IOHTAP server.",
    )
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to IOHTAP's configuration file.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Set to enable debug logging.",
    )
    parser.set_defaults(func=main)


def main(args):
    should_shutdown = threading.Event()

    def shutdown_signal_handler(_sig, _frame):
        should_shutdown.set()

    signal.signal(signal.SIGINT, shutdown_signal_handler)
    signal.signal(signal.SIGTERM, shutdown_signal_handler)

    config = ConfigFile(args.config_file)
    set_up_logging(debug_mode=args.debug)

    with IOHTAPServer(config):
        # Run until asked to terminate.
        should_shutdown.wait()
