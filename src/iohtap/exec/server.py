import logging
import signal
import threading

from iohtap.config.file import ConfigFile
from iohtap.server.server import IOHTAPServer


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

    # Configure the logger.
    logging_kwargs = {
        "format": "%(asctime)s %(levelname)-8s %(message)s",
        "datefmt": "%Y-%m-%d %H:%M",
        "level": logging.DEBUG if args.debug else logging.INFO,
    }
    logging.basicConfig(**logging_kwargs)

    with IOHTAPServer(config):
        # Run until asked to terminate.
        should_shutdown.wait()
