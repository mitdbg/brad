import cmd
import pathlib
import readline
import time
import pyodbc
from typing import List, Tuple
from tabulate import tabulate

import brad
from brad.config.strings import SHELL_HISTORY_FILE
from brad.grpc_client import BradGrpcClient, BradClientError
from brad.flight_sql_client_odbc import BradFlightSqlClientOdbc


def register_command(subparsers):
    parser = subparsers.add_parser(
        "cli",
        help="Start a BRAD client session.",
    )
    parser.add_argument(
        "-c",
        "--command",
        type=str,
        help="Run a single SQL query (or internal command) and exit.",
    )
    parser.add_argument(
        "--use-odbc", action="store_true", help="Use the ODBC endpoint instead of gRPC."
    )
    parser.add_argument(
        "endpoint",
        nargs="?",
        help="The BRAD endpoint to connect to. Defaults to localhost:6583.",
        default="localhost:6583",
    )
    parser.set_defaults(func=main)


def parse_endpoint(endpoint: str) -> Tuple[str, int]:
    parts = endpoint.split(":")
    if len(parts) != 2:
        raise ValueError("Invalid endpoint format.")
    return parts[0], int(parts[1])


def run_command(args) -> None:
    host, port = parse_endpoint(args.endpoint)
    if args.use_odbc:
        with BradFlightSqlClientOdbc(host, port) as client:
            run_query(client, args.command)
    else:
        with BradGrpcClient(host, port) as client:
            run_query(client, args.command)


def run_query(client: BradGrpcClient | BradFlightSqlClientOdbc, query: str) -> None:
    try:
        # Dispatch query and print results. We buffer the whole result
        # set in memory to get a reasonable estimate of the query
        # execution time (including network overheads).
        exec_engine = None
        start = time.time()
        results, exec_engine, not_tabular = client.run_query_json_cli(query)
        end = time.time()

        if not_tabular:
            for line in results:
                if len(line) == 1:
                    print(line[0])
                else:
                    print(line)
        else:
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
    except pyodbc.Error as ex:
        print()
        print("Query resulted in an error:")
        print(repr(ex))
        print()


class BradShell(cmd.Cmd):
    READY_PROMPT = ">>> "
    MULTILINE_PROMPT = "--> "
    TERM_STR = ";"

    def __init__(self, client: BradGrpcClient | BradFlightSqlClientOdbc) -> None:
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

    host, port = parse_endpoint(args.endpoint)
    print("BRAD Interactive Shell v{}".format(brad.__version__))
    print()
    if args.use_odbc:
        print("Connecting to BRAD VDBE at {}:{} (using ODBC)...".format(host, port))
    else:
        print("Connecting to BRAD VDBE at {}:{}...".format(host, port))

    def run_shell(client: BradGrpcClient | BradFlightSqlClientOdbc) -> None:
        print("Connected!")
        print()
        print("Terminate all SQL queries with a semicolon (;). Hit Ctrl-D to exit.")
        print("Enter 'help' or '?' for details on additional commands.")
        print()

        shell = BradShell(client)
        shell.cmdloop()

    if args.use_odbc:
        with BradFlightSqlClientOdbc(host, port) as client:
            run_shell(client)
    else:
        with BradGrpcClient(host, port) as client:
            run_shell(client)
