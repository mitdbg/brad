import argparse
import ast
from collections import defaultdict
from typing import Any
import numpy as np
import pandas as pd
import time

from brad.grpc_client import BradGrpcClient
from brad.flight_sql_client_odbc import BradFlightSqlClientOdbc
from brad.sqlite_client import BradSqliteClient


def adjusted_data(data: list[float], drop_count=1) -> list[float]:
    # Drop top and bottom `k` values from `data`
    return sorted(data)[drop_count : len(data) - drop_count]


def run_client(
    client: BradGrpcClient | BradFlightSqlClientOdbc | BradSqliteClient,
    trials: int,
    repetitions: int,
    query: str,
) -> tuple[np.floating[Any], np.floating[Any]]:
    average_latencies = []
    for _ in range(trials):
        start = time.time()
        for _ in range(repetitions):
            if isinstance(client, BradGrpcClient):
                client.run_query_json(query)
            else:
                client.run_query(query)
        end = time.time()

        total = end - start
        latency = total / repetitions

        average_latencies.append(latency)

    adjusted_average_latencies = adjusted_data(average_latencies)
    return np.mean(adjusted_average_latencies), np.std(adjusted_average_latencies)


def build_dataframe() -> pd.DataFrame:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repetitions", type=int, default=1000)
    parser.add_argument("--trials", type=int, default=10)
    parser.add_argument("--query", type=str, default="SELECT 1")
    args = parser.parse_args()

    queries = [
        "SELECT 1;",
        "SELECT * FROM person_info;",
        "SELECT * FROM person_info WHERE id = 123456;",
    ]

    data = defaultdict(list)

    data["Connection Type"].append("gRPC")
    with BradGrpcClient(host="localhost", port=6583) as client:
        for query in queries:
            lat_avg, lat_std_dev = run_client(
                client, args.trials, args.repetitions, query
            )
            data[query].append(str((lat_avg, lat_std_dev)))

    data["Connection Type"].append("Flight SQL ODBC")
    with BradFlightSqlClientOdbc(host="localhost", port=31337) as client:
        for query in queries:
            lat_avg, lat_std_dev = run_client(
                client, args.trials, args.repetitions, query
            )
            data[query].append(str((lat_avg, lat_std_dev)))

    data["Connection Type"].append("SQLite")
    with BradSqliteClient(database="/tmp/sophiez_brad_stub_db.sqlite") as client:
        for query in queries:
            lat_avg, lat_std_dev = run_client(
                client, args.trials, args.repetitions, query
            )
            data[query].append(str((lat_avg, lat_std_dev)))

    return pd.DataFrame.from_dict(data)


def print_to_csv(dataframe: pd.DataFrame, filename: str) -> None:
    dataframe.to_csv(filename, index=False)


def plot_from_csv(filename: str) -> None:
    dataframe = pd.read_csv(filename)

    lat_avgs = {}
    lat_std_devs = {}
    for query in dataframe.columns[1:]:
        query_records = dataframe[query].tolist()
        lat_avgs[query] = [
            ast.literal_eval(lat_statistic)[0] for lat_statistic in query_records
        ]
        lat_std_devs[query] = [
            ast.literal_eval(lat_statistic)[1] for lat_statistic in query_records
        ]

    std_dev_yerr = []
    for query, std_devs in lat_std_devs.items():
        std_dev_yerr.append([std_devs, std_devs])

    df = pd.DataFrame(lat_avgs, index=dataframe["Connection Type"].tolist())
    ax = df.plot.bar(yerr=std_dev_yerr, rot=0)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.25))
    ax.set_xlabel("Connection Type")
    ax.set_ylabel("Average Latency (s)")

    fig = ax.get_figure()
    fig.savefig("measurement_comparisons_plot.png", bbox_inches="tight")


def main() -> None:
    dataframe = build_dataframe()
    csv_filename = "measurement_comparisons.csv"
    print_to_csv(dataframe, csv_filename)
    plot_from_csv(csv_filename)


if __name__ == "__main__":
    main()
