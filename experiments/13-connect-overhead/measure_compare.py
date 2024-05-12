import argparse
import time
from brad.grpc_client import BradGrpcClient
from brad.flight_sql_client_odbc import BradFlightSqlClientOdbc
from brad.sqlite_client import BradSqliteClient
import pandas
import numpy as np
from collections import defaultdict

def run_client(client, trials, repetitions, query) -> tuple[float, float]:
    total_lat = 0
    lats = []
    for _ in range(trials):
        start = time.time()
        for _ in range(repetitions):
            if isinstance(client, BradGrpcClient):
                client.run_query_json(query)
            else:
                client.run_query(query)
        end = time.time()

        total = end - start
        lat = total / repetitions

        lats.append(lat)
        total_lat += lat
    
    return total_lat / trials, np.std(lats)


def build_dataframe() -> pandas.DataFrame:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repetitions", type=int, default=10000)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--query", type=str, default="SELECT 1")
    args = parser.parse_args()

    queries = [
        "SELECT 1;",
        "SELECT * FROM person_info;",
        "SELECT * FROM person_info WHERE id = 123456;"
    ]

    data = defaultdict(list)

    data["Connection Type"].append("gRPC")
    with BradGrpcClient(host="localhost", port=6583) as client:
        for query in queries:
            lat_avg, lat_std_dev = run_client(client, args.trials, args.repetitions, query)
            data[f"Latency (Avg, Std Dev) for `{query}`"].append((lat_avg, lat_std_dev))
        
    data["Connection Type"].append("Flight SQL ODBC")
    with BradFlightSqlClientOdbc(host="localhost", port=31337) as client:
        for query in queries:
            lat_avg, lat_std_dev = run_client(client, args.trials, args.repetitions, query)
            data[f"Latency (Avg, Std Dev) for `{query}`"].append((lat_avg, lat_std_dev))

    data["Connection Type"].append("SQLite")
    with BradSqliteClient(database="/tmp/sophiez_brad_stub_db.sqlite") as client:
        for query in queries:
            lat_avg, lat_std_dev = run_client(client, args.trials, args.repetitions, query)
            data[f"Latency (Avg, Std Dev) for `{query}`"].append((lat_avg, lat_std_dev))

    return pandas.DataFrame.from_dict(data)

def print_to_csv(dataframe: pandas.DataFrame, filename: str) -> None:
    dataframe.to_csv(filename, index=False)

def plot_from_csv(filename: str) -> None:
    dataframe = pandas.read_csv(filename)
    print(dataframe)

def main():
    dataframe = build_dataframe()
    csv_filename = "measurement_comparisons.csv"
    print_to_csv(dataframe, csv_filename)
    plot_from_csv(csv_filename)

if __name__ == "__main__":
    main()
