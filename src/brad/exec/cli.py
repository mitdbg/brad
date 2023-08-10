import cmd
import pathlib
import readline
import time
from typing import List
from tabulate import tabulate

import brad
from brad.config.strings import SHELL_HISTORY_FILE
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
    parser.add_argument(
        "-c",
        "--command",
        type=str,
        help="Run a single SQL query (or internal command) and exit.",
    )
    parser.set_defaults(func=main)


def run_command(args) -> None:
    with BradGrpcClient(args.host, args.port) as client:
        run_query(client, args.command)


def run_query(client: BradGrpcClient, query: str) -> None:
    try:
        # Dispatch query and print results. We buffer the whole result
        # set in memory to get a reasonable estimate of the query
        # execution time (including network overheads).
        exec_engine = None
        start = time.time()
        results, exec_engine = client.run_query_json(query)
        end = time.time()

        print(tabulate(results, tablefmt="simple_grid"))
        print()
        if exec_engine is not None:
            print("Took {:.3f} seconds. Ran on {}".format(end - start, exec_engine))
        else:
            print("Took {:.3f} seconds.".format(end - start))
        print()
    except BradClientError as ex:
        print()
        print("Query resulted in an error:")
        print(ex.message())
        print()


class BradShell(cmd.Cmd):
    READY_PROMPT = ">>> "
    MULTILINE_PROMPT = "--> "
    TERM_STR = ";"

    def __init__(self, client: BradGrpcClient) -> None:
        super().__init__()
        self.prompt = self.READY_PROMPT
        self._client = client
        self._multiline_parts: List[str] = []

        self._history_file = pathlib.Path.home() / SHELL_HISTORY_FILE
        readline.set_history_length(1000)
        try:
            readline.read_history_file(self._history_file)
        except FileNotFoundError:
            pass

    def default(self, line: str) -> None:
        if line == "EOF":
            print()
            return True  # type: ignore

        line = line.strip()
        if line.endswith(self.TERM_STR):
            if len(self._multiline_parts) > 0:
                self._multiline_parts.append(line)
                full_query = " ".join(self._multiline_parts)
                self._multiline_parts.clear()
                self.prompt = self.READY_PROMPT
            else:
                full_query = line
            self._run_query(full_query)
        else:
            self._multiline_parts.append(line)
            self.prompt = self.MULTILINE_PROMPT

    def do_help(self, _command: str) -> None:
        print("Type SQL queries and hit enter to submit them to BRAD.")
        print("Terminate all SQL queries with a semicolon (;). Hit Ctrl-D to exit.")
        print()

    def cmdloop(self, intro=None) -> None:
        try:
            super().cmdloop(intro)
        except (KeyboardInterrupt, EOFError):
            # Graceful shutdown.
            pass
        finally:
            readline.write_history_file(self._history_file)

    def _run_query(self, query: str) -> None:
        run_query(self._client, query)


def main(args) -> None:
    if args.command is not None:
        run_command(args)
        return

    print("BRAD Interactive Shell v{}".format(brad.__version__))
    print()
    print("Connecting to BRAD at {}:{}...".format(args.host, args.port))

    with BradGrpcClient(args.host, args.port) as client:
        print("Connected!")
        print()
        print("Terminate all SQL queries with a semicolon (;). Hit Ctrl-D to exit.")
        print("Enter 'help' or '?' for details on additional commands.")
        print()

        shell = BradShell(client)
        shell.cmdloop()
