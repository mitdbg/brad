import argparse
import time
from brad.grpc_client import BradGrpcClient
from brad.flight_sql_client_odbc import BradFlightSqlClientOdbc
from brad.flight_sql_client import BradFlightSqlClient
from prettytable import PrettyTable


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repetitions", type=int, default=10000)
    parser.add_argument("--query", type=str, default="SELECT 1")
    args = parser.parse_args()

    table = PrettyTable()
    table.field_names = ["Connection Type", "Reps", "Total Time (s)", "Average Latency (s)"]

    # TODO: Remove hardcoding
    with BradGrpcClient(host="localhost", port=6583) as client:
        start = time.time()
        for _ in range(args.repetitions):
            client.run_query_json(args.query)
        end = time.time()

    total = end - start
    avg_lat = total / args.repetitions

    table.add_row(["gRPC", args.repetitions, total, avg_lat])

    with BradFlightSqlClientOdbc(host="localhost", port=31337) as client:
        start = time.time()
        for _ in range(args.repetitions):
            client.run_query(args.query)
        end = time.time()

    total = end - start
    avg_lat = total / args.repetitions

    table.add_row(["Flight SQL ODBC", args.repetitions, total, avg_lat])

    with BradFlightSqlClient(database="/tmp/sophiez_brad_stub_db.sqlite") as client:
        start = time.time()
        for _ in range(args.repetitions):
            client.run_query(args.query)
        end = time.time()

    total = end - start
    avg_lat = total / args.repetitions

    table.add_row(["Flight SQL (direct)", args.repetitions, total, avg_lat])

    print(table)


if __name__ == "__main__":
    main()
