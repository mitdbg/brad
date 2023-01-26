import socket
import time

import iohtap


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
    print("IOHTAP Interactive CLI v{}".format(iohtap.__version__))
    print()
    print("Sending queries to IOHTAP at {}:{}.".format(args.host, args.port))
    print("Terminate all SQL queries with a semicolon (;). Hit Ctrl-D to exit.")
    print()

    while True:
        # Allow multiline input.
        try:
            pieces = []
            pieces.append(input(">>> ").strip())
            while not pieces[-1].endswith(";"):
                pieces.append(input("--> "))
            query = " ".join(pieces)
        except (EOFError, KeyboardInterrupt):
            break

        req_socket = socket.create_connection((args.host, args.port))
        with req_socket.makefile("rw") as io:
            start = time.time()
            print(query, file=io, flush=True)
            # Wait for and print the results.
            print()
            for line in io:
                print(line, end="")
            end = time.time()
            print()
            print("Took {:.3f} seconds.".format(end - start))
        req_socket.close()
        print()
