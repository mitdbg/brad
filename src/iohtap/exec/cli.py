import socket


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
        default=6583,
        help="The port on which IOHTAP is listening for connections.",
    )
    parser.set_defaults(func=main)


def main(args):
    req_socket = socket.create_connection((args.host, args.port))
    with req_socket.makefile("rw") as io:
        print("SELECT * FROM demo", file=io, flush=True)
        # Wait for and print the results.
        for line in io:
            print(line, end="")
    req_socket.close()
