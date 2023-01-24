from iohtap.config import DEFAULT_IOHTAP_DAEMON_PORT


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
        "--port",
        type=int,
        default=DEFAULT_IOHTAP_DAEMON_PORT,
        help="The port on which the IOHTAP server accepts daemon connections.",
    )
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to IOHTAP's configuration file.",
    )
    parser.set_defaults(func=main)


def main(args):
    print("Would connect to {}:{}".format(args.host, args.port))
