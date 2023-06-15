import time

import brad
from brad.grpc_client import BradGrpcClient, BradClientError


def register_command(subparsers):
    parser = subparsers.add_parser(
        "cli",
        help="Start a BRAD client session.",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="The host where the BRAD server is running.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=6583,
        help="The port on which BRAD is listening for connections.",
    )
    parser.set_defaults(func=main)


def main(args):
    print("BRAD Interactive CLI v{}".format(brad.__version__))
    print()
    print("Connecting to BRAD at {}:{}...".format(args.host, args.port))

    with BradGrpcClient(args.host, args.port) as client:
        print("Connected!")
        print()
        print("Terminate all SQL queries with a semicolon (;). Hit Ctrl-D to exit.")
        print()

        try:
            while True:
                # Allow multiline input.
                pieces = []
                pieces.append(input(">>> ").strip())
                while not pieces[-1].endswith(";"):
                    pieces.append(input("--> "))
                query = " ".join(pieces)

                try:
                    # Dispatch query and print results. We buffer the whole result
                    # set in memory to get a reasonable estimate of the query
                    # execution time (including network overheads).
                    encoded_rows = []
                    exec_engine = None
                    start = time.time()
                    encoded_row_stream = client.run_query(query)
                    for encoded_row, exec_engine in encoded_row_stream:
                        encoded_rows.append(encoded_row)
                    end = time.time()

                    for encoded_row in encoded_rows:
                        print(encoded_row.decode())
                    print()
                    if exec_engine is not None:
                        print(
                            "Took {:.3f} seconds. Ran on {}".format(
                                end - start, exec_engine
                            )
                        )
                    else:
                        print("Took {:.3f} seconds.".format(end - start))
                    print()
                except BradClientError as ex:
                    print()
                    print("Query resulted in an error:")
                    print(ex.message())
                    print()

        except (EOFError, KeyboardInterrupt):
            pass
