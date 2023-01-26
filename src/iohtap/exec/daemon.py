from iohtap.daemon.daemon import IOHTAPDaemon
from iohtap.config.file import ConfigFile
from iohtap.utils import set_up_logging


def register_command(subparsers):
    parser = subparsers.add_parser(
        "daemon",
        help="Start the IOHTAP background daemon.",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="The host on which the IOHTAP server is running.",
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
    config = ConfigFile(args.config_file)
    set_up_logging(debug_mode=args.debug)
    daemon = IOHTAPDaemon.connect(args.host, config)
    # NOTE: The daemon does not currently shut down gracefully.
    daemon.run()
