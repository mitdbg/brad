from iohtap.config import DEFAULT_IOHTAP_SERVER_PORT


def register_command(subparsers):
    parser = subparsers.add_parser(
        "cli",
        help="Start a IOHTAP client session.",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="The host where the IOHTAP server is running.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_IOHTAP_SERVER_PORT,
        help="The port on which IOHTAP is listening for connections.",
    )
    parser.set_defaults(func=main)


def main(args):
    pass
