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
    parser.set_defaults(func=main)


def main(args):
    config = ConfigFile(args.config_file)
    server = IOHTAPServer(config)
    server.handle_query("SELECT 1, 2, 3")
