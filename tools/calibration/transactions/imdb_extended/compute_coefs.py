import argparse
import pandas as pd
import numpy as np
import pathlib
import math
import sys
from typing import List, Tuple
from sklearn.linear_model import LinearRegression


def extract_metrics(metrics_file: str) -> pd.DataFrame:
    rel_metrics = [
        "os.loadAverageMinute.one.avg",
        "os.cpuUtilization.total.avg",
        "os.network.rx.avg",
        "os.network.tx.avg",
        "db.SQL.queries.avg",
        "db.SQL.total_query_time.avg",
        "db.SQL.tup_deleted.avg",
        "db.SQL.tup_fetched.avg",
        "db.SQL.tup_inserted.avg",
        "db.SQL.tup_returned.avg",
        "db.SQL.tup_updated.avg",
        "db.Transactions.active_transactions.avg",
        "db.Transactions.blocked_transactions.avg",
        "db.Transactions.duration_commits.avg",
        "db.Transactions.xact_commit.avg",
        "db.Transactions.xact_rollback.avg",
        "os.diskIO.auroraStorage.auroraStorageBytesRx.avg",
        "os.diskIO.auroraStorage.auroraStorageBytesTx.avg",
        "os.diskIO.auroraStorage.diskQueueDepth.avg",
        "os.diskIO.auroraStorage.readThroughput.avg",
        "os.diskIO.auroraStorage.writeThroughput.avg",
        "os.diskIO.auroraStorage.readLatency.avg",
        "os.diskIO.auroraStorage.writeLatency.avg",
        "os.diskIO.auroraStorage.readIOsPS.avg",
        "os.diskIO.auroraStorage.writeIOsPS.avg",
    ]
    df = pd.read_csv(metrics_file, index_col=0)
    df.index = pd.to_datetime(df.index)
    rel = df[rel_metrics]
    return rel.iloc[-2:]


def process_metrics(df: pd.DataFrame) -> pd.DataFrame:
    cpu = df["os.cpuUtilization.total.avg"].mean()

    exp_1 = math.exp(-1)
    exp_1_rest = 1 - exp_1
    load_last = df["os.loadAverageMinute.one.avg"].iloc[-1]
    load_2nd_last = df["os.loadAverageMinute.one.avg"].iloc[-2]
    load_minute = (load_last - exp_1 * load_2nd_last) / exp_1_rest

    load_avg = df["os.loadAverageMinute.one.avg"].mean()

    return pd.DataFrame(
        {
            "cpu": [cpu],
            "load_minute": [load_minute],
            "load_orig": [load_avg],
            "txn_commit": [df["db.Transactions.xact_commit.avg"].mean()],
            "txn_rollback": [df["db.Transactions.xact_rollback.avg"].mean()],
            "tup_fetch": [df["db.SQL.tup_fetched.avg"].mean()],
            "tup_insert": [df["db.SQL.tup_inserted.avg"].mean()],
            "tup_delete": [df["db.SQL.tup_deleted.avg"].mean()],
            "tup_update": [df["db.SQL.tup_updated.avg"].mean()],
            "txn_active": [df["db.Transactions.active_transactions.avg"].mean()],
            "io_read_thpt": [df["os.diskIO.auroraStorage.readThroughput.avg"].mean()],
            "io_write_thpt": [df["os.diskIO.auroraStorage.writeThroughput.avg"].mean()],
            "io_read_iops": [df["os.diskIO.auroraStorage.readIOsPS.avg"].mean()],
            "io_write_iops": [df["os.diskIO.auroraStorage.writeIOsPS.avg"].mean()],
            "disk_queue": [df["os.diskIO.auroraStorage.diskQueueDepth.avg"].mean()],
            "read_lat": [df["os.diskIO.auroraStorage.readLatency.avg"].mean()],
            "write_lat": [df["os.diskIO.auroraStorage.writeLatency.avg"].mean()],
        }
    )


def load_data(prefix):
    all_metrics = []
    all_lats = []
    all_stats = []

    for exp in pathlib.Path(prefix).iterdir():
        if not exp.is_dir() or exp.name.startswith("."):
            continue
        parts = exp.name.split("-")
        num_clients = int(parts[1])

        metrics_df = extract_metrics(exp / "pi_metrics.csv")
        metrics = process_metrics(metrics_df)
        metrics.insert(0, "num_clients", num_clients)
        all_metrics.append(metrics)

        for idx in range(num_clients):
            df = pd.read_csv(exp / "oltp_latency_{}.csv".format(idx))
            df.insert(0, "num_clients", num_clients)
            df.pop("txn_idx")
            all_lats.append(df)

        for idx in range(num_clients):
            stats_df = pd.read_csv(exp / "oltp_stats_{}.csv".format(idx))
            stats_df = stats_df.pivot_table(index=None, columns="stat", values="value")
            stats_df.index.name = None
            stats_df.insert(0, "num_clients", num_clients)
            all_stats.append(stats_df)

    comb_metrics = pd.concat(all_metrics, ignore_index=True)
    comb_metrics = comb_metrics.fillna(0.0)

    comb_lats = pd.concat(all_lats, ignore_index=True)
    comb_lats = comb_lats.groupby(["num_clients"]).mean().reset_index()

    comb_stats = pd.concat(all_stats, ignore_index=True)
    comb_stats = (
        comb_stats.groupby(["num_clients"])
        .agg(
            {
                "add_showing_aborts": "sum",
                "add_showing_commits": "sum",
                "edit_note_aborts": "sum",
                "edit_note_commits": "sum",
                "purchase_aborts": "sum",
                "purchase_commits": "sum",
                "overall_run_time_s": "max",
            }
        )
        .reset_index()
    )

    return (
        comb_metrics.sort_values(by=["num_clients"], ignore_index=True),
        comb_lats.sort_values(by=["num_clients"], ignore_index=True),
        comb_stats.sort_values(by=["num_clients"], ignore_index=True),
    )


def combine_data(
    data_path_str: str, instances: List[str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cpu_count = {
        "r6g_large": 2,
        "r6g_xlarge": 4,
        "r6g_2xlarge": 8,
        "r6g_4xlarge": 16,
    }
    results = []
    all_stats = []
    data_path = pathlib.Path(data_path_str)
    for inst in instances:
        metrics, lats, stats = load_data(data_path / inst)
        comb = pd.merge(lats, metrics, on=["num_clients"])
        comb.insert(0, "instance", inst)
        comb.insert(1, "num_cpus", cpu_count[inst])
        results.append(comb)

        stats.insert(0, "instance", inst)
        stats.insert(1, "num_cpus", cpu_count[inst])
        all_stats.append(stats)

    all_data = pd.concat(results, ignore_index=True)
    all_stats_df = pd.concat(all_stats, ignore_index=True)
    return all_data.sort_values(
        by=["num_cpus", "num_clients"]
    ), all_stats_df.sort_values(by=["num_cpus", "num_clients"])


def compute_client_txn_rate(df: pd.DataFrame) -> pd.DataFrame:
    df["completions"] = (
        df["add_showing_commits"]
        + df["add_showing_aborts"]
        + df["edit_note_commits"]
        + df["edit_note_aborts"]
        + df["purchase_commits"]
        + df["purchase_aborts"]
    )
    df["completions_per_s"] = df["completions"] / df["overall_run_time_s"]
    return df


def compute_diffs(load_data_df: pd.DataFrame) -> pd.DataFrame:
    instances = load_data_df["instance"].unique()
    all_data = []
    for inst in instances:
        rel = load_data_df[load_data_df["instance"] == inst].copy()
        rel["diff"] = rel["completions_per_s"] / rel["completions_per_s"].shift(1)
        rel = rel.dropna()
        all_data.append(rel)
    df = pd.concat(all_data, ignore_index=True)
    return df


def find_cutoffs(diffs: pd.DataFrame, threshold=1.2) -> pd.DataFrame:
    instances = diffs["instance"].unique()

    results = []

    for inst in instances:
        rel = diffs[diffs["instance"] == inst]
        m = rel["diff"].le(threshold)
        if (~m).all():
            print("Warning: Saturation point not detected for", inst, file=sys.stderr)
            idxmax = m.tail(1).index.item()
        else:
            idxmax = m.idxmax()
        load = rel["load_minute"].loc[idxmax]
        cpu = rel["cpu"].loc[idxmax]
        num_clients = rel["num_clients"].loc[idxmax]
        completion_rate = rel["completions_per_s"].loc[idxmax]
        tup_per_s = rel["tup_modified_per_s"].loc[idxmax]
        kiops_r = rel["io_read_iops"].loc[idxmax] / 1000
        kiops_w = rel["io_write_iops"].loc[idxmax] / 1000
        cpu_load = rel["cpu_load"].loc[idxmax]

        results.append(
            (
                inst,
                rel["num_cpus"].iloc[0],
                load,
                cpu,
                num_clients,
                completion_rate,
                tup_per_s,
                kiops_r,
                kiops_w,
                cpu_load,
            )
        )

    return pd.DataFrame.from_records(
        results,
        columns=[
            "instance",
            "num_cpus",
            "peak_load",
            "peak_cpu",
            "num_clients",
            "completions_per_s",
            "tup_modified_per_s",
            "kiops_r",
            "kiops_w",
            "cpu_load",
        ],
    )


def do_regression_xy(
    x, y, fit_intercept=True, positive=True
) -> Tuple[LinearRegression, float]:
    reg = LinearRegression(fit_intercept=fit_intercept, positive=positive)
    xs = np.array(x, dtype=float)
    ys = np.array(y, dtype=float)
    if len(xs.shape) == 1:
        xs = np.expand_dims(xs, 1)
    model = reg.fit(xs, ys)
    score = model.score(xs, ys)
    return model, score


def compute_cutoff_models(
    cutoffs: pd.DataFrame,
) -> Tuple[List[LinearRegression], List[float]]:
    cpu_m, cpu_s = do_regression_xy(
        cutoffs["num_cpus"], cutoffs["cpu_load"], fit_intercept=False
    )
    wiops_m, wiops_s = do_regression_xy(
        cutoffs["num_cpus"], cutoffs["kiops_w"] * 1000, fit_intercept=False
    )
    riops_m, riops_s = do_regression_xy(
        cutoffs["num_cpus"], cutoffs["kiops_r"] * 1000, fit_intercept=False
    )
    load_m, load_s = do_regression_xy(
        cutoffs["num_cpus"], cutoffs["peak_load"], fit_intercept=False
    )

    return [cpu_m, wiops_m, riops_m, load_m], [cpu_s, wiops_s, riops_s, load_s]


def load_translation(df: pd.DataFrame, rates: pd.DataFrame) -> pd.DataFrame:
    common = ["instance", "num_cpus", "num_clients"]
    j = pd.merge(
        df[
            [
                *common,
                "load_minute",
                "cpu",
                "tup_insert",
                "tup_delete",
                "tup_update",
                "io_read_iops",
                "io_write_iops",
                "disk_queue",
                "read_lat",
                "write_lat",
            ]
        ],
        rates[[*common, "completions_per_s", "overall_run_time_s"]],
        on=common,
    )
    j["tup_modified"] = j["tup_insert"] + j["tup_delete"] + j["tup_update"]
    j.pop("tup_insert")
    j.pop("tup_update")
    j.pop("tup_delete")
    j["tup_modified_per_s"] = j["tup_modified"] / j["overall_run_time_s"]
    j.pop("tup_modified")
    j.pop("overall_run_time_s")
    j["cpu_load"] = j["cpu"] / 100 * j["num_cpus"]
    return j


def filter_cutoff(
    ld: pd.DataFrame, cutoffs: pd.DataFrame, cutoff_metric="num_clients"
) -> pd.DataFrame:
    instances = ld["instance"].unique()
    data = []

    for inst in instances:
        rel = ld[ld["instance"] == inst].copy()
        cut_rel = cutoffs[cutoffs["instance"] == inst]
        cut_val = cut_rel[cutoff_metric].iloc[0]

        rel_good = rel[rel[cutoff_metric] <= cut_val]
        if len(rel_good) > 2:
            rel_good = rel_good[:-1]
        data.append(rel_good)

    return pd.concat(data, ignore_index=True)


def compute_translation_models(
    fld: pd.DataFrame,
) -> Tuple[List[LinearRegression], List[float]]:
    kwargs = {
        "fit_intercept": False,
    }
    cpu_load = do_regression_xy(fld["completions_per_s"], fld["cpu_load"], **kwargs)
    io_write = do_regression_xy(
        fld["completions_per_s"], fld["io_write_iops"], **kwargs
    )
    io_read = do_regression_xy(fld["completions_per_s"], fld["io_read_iops"], **kwargs)
    load = do_regression_xy(fld["completions_per_s"], fld["load_minute"], **kwargs)

    return [cpu_load[0], io_write[0], io_read[0], load[0]], [
        cpu_load[1],
        io_write[1],
        io_read[1],
        load[1],
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-data-path", type=str, required=True)
    parser.add_argument("--saturation-threshold", type=float, default=1.2)
    args = parser.parse_args()

    data, stats = combine_data(
        args.raw_data_path, ["r6g_large", "r6g_xlarge", "r6g_2xlarge", "r6g_4xlarge"]
    )

    rates = compute_client_txn_rate(stats)
    load_data_df = load_translation(data, rates)

    # Compute the "saturation load" for each provisioning.
    cutoffs = find_cutoffs(
        compute_diffs(load_data_df), threshold=args.saturation_threshold
    )
    cutoff_models, cutoff_scores = compute_cutoff_models(cutoffs)

    # Compute "translation models" (mapping client-side metrics to system metrics)
    fld = filter_cutoff(load_data_df, cutoffs)
    translation_models, translation_scores = compute_translation_models(fld)

    # Print out the results.
    print("client_thpt_to_cpu_denorm:", translation_models[0].coef_.item())
    print("client_thpt_to_io_write:", translation_models[1].coef_.item())
    print("client_thpt_to_io_read:", translation_models[2].coef_.item())
    print("client_thpt_to_load:", translation_models[3].coef_.item())
    print()
    print("prov_to_peak_cpu_denorm:", cutoff_models[0].coef_.item())
    print("prov_to_peak_io_write:", cutoff_models[1].coef_.item())
    print("prov_to_peak_io_read:", cutoff_models[2].coef_.item())
    print("prov_to_peak_load:", cutoff_models[3].coef_.item())
    print()

    # Sanity check.
    for m in translation_models:
        assert m.intercept_ == 0
    for m in cutoff_models:
        assert m.intercept_ == 0

    # Print out R^2 scores
    print("R^2 Scores")
    print("client_thpt_to_cpu_denorm:", translation_scores[0])
    print("client_thpt_to_io_write:", translation_scores[1])
    print("client_thpt_to_io_read:", translation_scores[2])
    print("client_thpt_to_load:", translation_scores[3])
    print()
    print("prov_to_peak_cpu_denorm:", cutoff_scores[0])
    print("prov_to_peak_io_write:", cutoff_scores[1])
    print("prov_to_peak_io_read:", cutoff_scores[2])
    print("prov_to_peak_load:", cutoff_scores[3])
    print()


if __name__ == "__main__":
    main()
