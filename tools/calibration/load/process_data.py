import argparse
import pathlib
import pandas as pd
import numpy as np


def load_metrics(metrics_file: str) -> pd.DataFrame:
    metrics = {
        "redshift_CPUUtilization_Average": "cpu",
        "redshift_ReadIOPS_Average": "riops",
    }

    df = pd.read_csv(metrics_file, index_col=0)

    # Drop rows where all rows are 0
    df = df.replace(0.0, np.nan).dropna(how="all").fillna(0.0)
    assert len(df) >= 2

    # Take the average of the last two rows only
    df = df.iloc[-2:]

    rel = df[list(metrics.keys())]
    rel = rel.rename(columns=metrics)
    rel = rel.mean().to_frame().transpose()
    return rel


def load_data(data_dir: str) -> pd.DataFrame:
    run_times = []
    all_metrics = []

    for eng_dir in pathlib.Path(data_dir).iterdir():
        if not eng_dir.is_dir() or eng_dir.name.startswith("."):
            continue

        for exp_inst in eng_dir.iterdir():
            parts = exp_inst.name.split("-")
            instance = parts[0]
            num_nodes = int(parts[1])
            num_clients = int(parts[2])
            query_idx = int(parts[3][1:])

            # Recorded run time
            data = []
            for client in range(num_clients):
                df = pd.read_csv(exp_inst / f"runner_{client}.csv")
                data.append(df)
            data = pd.concat(data, ignore_index=True)
            data = data.groupby("query_idx").mean().reset_index()

            data.insert(0, "instance", instance)
            data.insert(1, "num_nodes", num_nodes)
            data.insert(2, "num_clients", num_clients)
            run_times.append(data)

            # Metrics
            metrics = load_metrics(exp_inst / "metrics.csv")
            metrics.insert(0, "instance", instance)
            metrics.insert(1, "num_nodes", num_nodes)
            metrics.insert(2, "num_clients", num_clients)
            metrics.insert(3, "query_idx", query_idx)
            all_metrics.append(metrics)

    cols = ["instance", "num_nodes", "num_clients", "query_idx"]
    rt = pd.concat(run_times)
    m = pd.concat(all_metrics)

    return rt.sort_values(by=cols, ignore_index=True), m.sort_values(
        by=cols, ignore_index=True
    )


def filter_data(df: pd.DataFrame) -> pd.DataFrame:
    # The measured data can be noisy. This filters out query samples where the
    # CPU utilization does not increase monotonically with respect to the number
    # of clients submitting requests to the engine.

    clean = []
    total = 0
    num_bad = 0
    instances = df["instance"].unique()

    for instance in instances:
        rel = df[df["instance"] == instance]
        nodes = rel["num_nodes"].unique()

        for n in nodes:
            rel2 = rel[rel["num_nodes"] == n]
            queries = rel2["query_idx"].unique()

            for q in queries:
                rel3 = rel2[rel2["query_idx"] == q].copy()
                rel3 = rel3.sort_values(by=["num_clients"])
                cpu_diffs = rel3["cpu"].diff()
                cpu_diffs = cpu_diffs.dropna()

                run_time_diffs = rel3["run_time_s"].diff()
                run_time_diffs = run_time_diffs.dropna()
                if (cpu_diffs >= 0).all():
                    clean.append(rel3)
                else:
                    num_bad += 1
                total += 1

    return pd.concat(clean, ignore_index=True), num_bad, total


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-data-path", type=str, required=True)
    parser.add_argument("--output-file", type=str, required=True)
    parser.add_argument("--run-filter", action="store_true")
    args = parser.parse_args()

    data, metrics = load_data(args.raw_data_path)
    all_data = pd.merge(
        data, metrics, on=["instance", "num_nodes", "num_clients", "query_idx"]
    )

    if args.run_filter:
        all_data, num_bad, total = filter_data(all_data)
        print(f"Removed {num_bad} queries out of a total of {total}.")

    all_data.to_csv(args.output_file, index=False)


if __name__ == "__main__":
    main()
