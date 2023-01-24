from iohtap.config import DEFAULT_IOHTAP_SERVER_PORT

def register_command(subparsers):
    parser = subparsers.add_parser(
        "server",
        help="Start the IOHTAP server.",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="The interface on which to listen for connections.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_IOHTAP_SERVER_PORT,
        help="The port on which to listen for connections.",
    )
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to IOHTAP's configuration file.",
    )
    parser.set_defaults(func=main)


def main(args):
    pass
