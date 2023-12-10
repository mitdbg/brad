import pathlib
from typing import Optional, Tuple

import pandas as pd


def load_txn_data(
    data_dir: pathlib.Path, num_clients: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    all_lats = []
    all_stats = []

    for exp_file in data_dir.iterdir():
        if exp_file.name.startswith("oltp_latency_"):
            df = pd.read_csv(exp_file)
            df.pop("txn_idx")
            df.insert(0, "num_clients", num_clients)
            all_lats.append(df)

        elif exp_file.name.startswith("oltp_stats_"):
            stats_df = pd.read_csv(exp_file)
            stats_df = stats_df.pivot_table(index=None, columns="stat", values="value")
            stats_df.index.name = None
            stats_df.insert(0, "num_clients", num_clients)
            all_stats.append(stats_df)

    if len(all_lats) == 0 or len(all_stats) == 0:
        return None, None

    comb_lats = pd.concat(all_lats, ignore_index=True)

    comb_stats = pd.concat(all_stats, ignore_index=True)
    comb_stats = (
        comb_stats.groupby("num_clients")
        .agg(
            {
                "purchase_aborts": "sum",
                "add_showing_aborts": "sum",
                "edit_note_aborts": "sum",
                "purchase_commits": "sum",
                "add_showing_commits": "sum",
                "edit_note_commits": "sum",
                "overall_run_time_s": "max",
            }
        )
        .reset_index()
    )

    return (comb_stats, comb_lats)


class RecordedRun:
    @classmethod
    def load(cls, exp_dir: str) -> "RecordedRun":
        base = pathlib.Path(exp_dir)

        if (base / "brad_metrics_front_end.log").exists():
            txn_thpt = pd.read_csv(base / "brad_metrics_front_end.log")
            txn_thpt["timestamp"] = pd.to_datetime(txn_thpt["timestamp"])
        else:
            txn_thpt = None

        stats = []
        ind_lats = []

        for inner in base.iterdir():
            if not inner.is_dir() or not inner.name.startswith("t_"):
                continue
            clients = int(inner.name.split("_")[1])
            oltp_stats, oltp_ind_lats = load_txn_data(inner, clients)
            stats.append(oltp_stats)
            ind_lats.append(oltp_ind_lats)

        oltp_stats = pd.concat(stats).sort_values(by=["num_clients"])
        oltp_ind_lats = pd.concat(ind_lats)

        all_olap = []

        for inner in base.iterdir():
            if not inner.is_dir() or not inner.name.startswith("ra_"):
                continue
            if inner.name == "ra_vector":
                clients = 2
            else:
                clients = int(inner.name.split("_")[1])
            for c in range(clients):
                olap_inner = pd.read_csv(
                    inner / "repeating_olap_batch_{}.csv".format(c)
                )
                olap_inner["timestamp"] = pd.to_datetime(olap_inner["timestamp"])
                olap_inner.insert(0, "num_clients", clients)
                all_olap.append(olap_inner)

        olap = pd.concat(all_olap).sort_values(by=["timestamp"])

        if (base / "brad_daemon_events.csv").exists():
            events = pd.read_csv(base / "brad_daemon_events.csv")
            events["timestamp"] = pd.to_datetime(events["timestamp"])
        else:
            events = None

        if "run_time_s" not in olap.columns:
            olap = olap.rename(columns={"run_time": "run_time_s"})

        return cls(txn_thpt, olap, oltp_stats, events, oltp_ind_lats)

    def __init__(
        self,
        txn_thpt: Optional[pd.DataFrame],
        olap_latency: pd.DataFrame,
        txn_stats: pd.DataFrame,
        events: Optional[pd.DataFrame],
        txn_lats: pd.DataFrame,
    ) -> None:
        self.txn_thpt = txn_thpt
        self.olap_latency = olap_latency
        self.txn_stats = txn_stats
        self.events = events
        self.txn_lats = txn_lats
