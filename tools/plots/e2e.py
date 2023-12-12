import pathlib
from typing import Optional, Tuple, List

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

        return cls(oltp_ind_lats, olap, oltp_stats, txn_thpt, events)

    def __init__(
        self,
        txn_lats: pd.DataFrame,
        olap_latency: pd.DataFrame,
        txn_stats: pd.DataFrame,
        txn_thpt: Optional[pd.DataFrame],
        events: Optional[pd.DataFrame],
    ) -> None:
        self.txn_lats = txn_lats
        self.olap_latency = olap_latency
        self.txn_stats = txn_stats
        self.txn_thpt = txn_thpt
        self.events = events

        self._txn_lat_p90: Optional[pd.DataFrame] = None
        self._ana_lat_p90: Optional[pd.DataFrame] = None
        self._timestamp_offsets: Optional[Tuple[pd.Timestamp, pd.Timestamp]] = None
        self._blueprint_intervals: Optional[List[Tuple[float, float]]] = None

    @property
    def is_brad(self) -> bool:
        # We only record daemon events on BRAD.
        return self.events is not None

    @property
    def txn_lat_p90(self) -> pd.DataFrame:
        if self._txn_lat_p90 is not None:
            return self._txn_lat_p90
        self._txn_lat_p90 = self._agg_txn_lats(0.9)
        return self._txn_lat_p90

    @property
    def ana_lat_p90(self) -> pd.DataFrame:
        if self._ana_lat_p90 is not None:
            return self._ana_lat_p90
        self._ana_lat_p90 = self._agg_ana_lats(0.9)
        return self._ana_lat_p90

    @property
    def timestamp_offsets(self) -> Tuple[pd.Timestamp, pd.Timestamp]:
        if self._timestamp_offsets is not None:
            return self._timestamp_offsets
        self._timestamp_offsets = self._compute_timestamp_offsets()
        return self._timestamp_offsets

    @property
    def blueprint_intervals(self) -> List[Tuple[float, float]]:
        if self._blueprint_intervals is not None:
            return self._blueprint_intervals
        self._blueprint_intervals = self._compute_blueprint_intervals()
        return self._blueprint_intervals

    def _agg_txn_lats(self, quantile: float) -> pd.DataFrame:
        ts = pd.to_datetime(self.txn_lats["timestamp"])
        il = self.txn_lats[["num_clients", "run_time_s"]]
        return (
            il.groupby([ts.dt.hour, ts.dt.minute])
            .quantile(quantile)
            .reset_index(drop=True)
        )

    def _agg_ana_lats(self, quantile: float) -> pd.DataFrame:
        ts = pd.to_datetime(self.olap_latency["timestamp"])
        il = self.olap_latency[["query_idx", "run_time_s"]]
        return (
            il.groupby([ts.dt.hour, ts.dt.minute])
            .quantile(quantile)
            .reset_index(drop=True)
        )

    def _compute_timestamp_offsets(self) -> Tuple[pd.Timestamp, pd.Timestamp]:
        if self.txn_thpt is not None:
            rel = self.txn_thpt.loc[self.txn_thpt["txn_end_per_s"] > 0]
            start_ts = rel.iloc[0]["timestamp"]
            end_ts = rel.iloc[-1]["timestamp"]
        else:
            print("Using txn latency to establish timestamp offset")
            start_ts = self.txn_lats.iloc[0]["timestamp"]
            end_ts = self.txn_lats.iloc[-1]["timestamp"]

        return pd.to_datetime(start_ts).tz_localize(None), pd.to_datetime(
            end_ts
        ).ts.localize(None)

    def _compute_blueprint_intervals(self) -> List[Tuple[float, float]]:
        if self.events is None:
            return []
        start_ts, end_ts = self.timestamp_offsets

        rel = self.events[self.events["event"] == "post_transition_completed"]
        rel = rel.sort_values(by=["timestamp"], ascending=True, ignore_index=True)
        rel["offset"] = rel["timestamp"] - start_ts
        rel["offset_minute"] = rel["offset"].dt.total_seconds() / 60.0

        last_offset = None
        intervals = []
        for offset_minute in rel["offset_minute"]:
            if last_offset is None:
                intervals.append((0.0, offset_minute))
            else:
                intervals.append((last_offset, offset_minute))
            last_offset = offset_minute
        if last_offset is not None:
            end_offset_minute = (end_ts - start_ts).dt.total_seconds() / 60.0
            intervals.append((last_offset, end_offset_minute))
        return intervals
