import pathlib
import pytz
import pickle
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

from brad.config.planner import PlannerConfig
from brad.planner.compare.blueprint import ComparableBlueprint


class RecordedRun:
    @classmethod
    def load(cls, exp_dir: str) -> "RecordedRun":
        base = pathlib.Path(exp_dir)

        if (base / "brad_metrics_front_end.log").exists():
            txn_thpt = pd.read_csv(base / "brad_metrics_front_end.log")
            txn_thpt["timestamp"] = pd.to_datetime(txn_thpt["timestamp"])
            txn_thpt["timestamp"] = txn_thpt["timestamp"].dt.tz_localize(None)
        else:
            txn_thpt = None

        stats = []
        ind_lats = []

        for inner in base.iterdir():
            if not inner.is_dir() or not inner.name.startswith("t_"):
                continue
            clients = int(inner.name.split("_")[1])
            oltp_stats, oltp_ind_lats = _load_txn_data(inner, clients)
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
                olap_inner["timestamp"] = olap_inner["timestamp"].dt.tz_localize(None)
                olap_inner.insert(0, "num_clients", clients)
                all_olap.append(olap_inner)

        olap = pd.concat(all_olap).sort_values(by=["timestamp"])

        if (base / "brad_daemon_events.csv").exists():
            events = pd.read_csv(base / "brad_daemon_events.csv")
            events["timestamp"] = pd.to_datetime(events["timestamp"])
            events["timestamp"] = events["timestamp"].dt.tz_localize(None)
        else:
            events = None

        if "run_time_s" not in olap.columns:
            olap = olap.rename(columns={"run_time": "run_time_s"})

        blueprints = _load_blueprints(base)

        return cls(oltp_ind_lats, olap, oltp_stats, txn_thpt, events, blueprints)

    def __init__(
        self,
        txn_lats: pd.DataFrame,
        olap_lats: pd.DataFrame,
        txn_stats: pd.DataFrame,
        txn_thpt: Optional[pd.DataFrame],
        events: Optional[pd.DataFrame],
        blueprints: List[Tuple[datetime, ComparableBlueprint]],
    ) -> None:
        self.txn_lats = txn_lats
        self.olap_lats = olap_lats
        self.txn_stats = txn_stats
        self.txn_thpt = txn_thpt
        self.events = events
        self.blueprints = blueprints

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

    def print_routing_breakdowns(self) -> None:
        olap_offsets = self.olap_lats.copy()
        olap_offsets["offset"] = olap_offsets["timestamp"] - self.timestamp_offsets[0]
        olap_offsets["offset_minute"] = olap_offsets["offset"].dt.total_seconds() / 60.0

        for idx, (offset_min, offset_max) in enumerate(self.blueprint_intervals):
            rel = olap_offsets[
                (olap_offsets["offset_minute"] >= offset_min)
                & (olap_offsets["offset_minute"] <= offset_max)
            ]
            is_redshift = (rel["engine"] == "redshift").sum()
            is_aurora = (rel["engine"] == "aurora").sum()
            is_athena = (rel["engine"] == "athena").sum()
            total = len(rel)

            print(f"Region {idx}: {str((offset_min, offset_max))}")
            print("Redshift: {}% ({})".format(is_redshift / total * 100, is_redshift))
            print("Aurora: {}% ({})".format(is_aurora / total * 100, is_aurora))
            print("Athena: {}% ({})".format(is_athena / total * 100, is_athena))
            print()

    def print_blueprint_costs(self) -> None:
        blueprints = self.blueprints
        start_ts_pd, _ = self.timestamp_offsets
        start_ts = start_ts_pd.to_pydatetime()

        for idx, (bp_ts, bp) in enumerate(blueprints):
            offset = (bp_ts - start_ts).total_seconds() / 60.0
            print(f"Blueprint {idx} (selected at offset {offset} mins)")
            print("Aurora:", bp.get_aurora_provisioning())
            print("Redshift:", bp.get_redshift_provisioning())
            print("Operational cost ($/hr):", bp.get_operational_monetary_cost())
            print()

    def _agg_txn_lats(self, quantile: float) -> pd.DataFrame:
        ts = pd.to_datetime(self.txn_lats["timestamp"])
        il = self.txn_lats[["num_clients", "run_time_s"]]
        return (
            il.groupby([ts.dt.hour, ts.dt.minute])
            .quantile(quantile)
            .reset_index(drop=True)
        )

    def _agg_ana_lats(self, quantile: float) -> pd.DataFrame:
        ts = pd.to_datetime(self.olap_lats["timestamp"])
        il = self.olap_lats[["query_idx", "run_time_s"]]
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

        return (
            pd.to_datetime(start_ts).tz_localize(None),
            pd.to_datetime(end_ts).tz_localize(None),
        )

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
            end_offset_minute = (end_ts - start_ts).total_seconds() / 60.0
            intervals.append((last_offset, end_offset_minute))
        return intervals

    def compute_olap_costs(
        self, olap_lats: pd.DataFrame, bytes_scanned_file: pathlib.Path
    ) -> pd.DataFrame:
        planner_config = PlannerConfig.load_only_constants()
        bytes_scanned = np.load(bytes_scanned_file)
        start_ts, _ = self.timestamp_offsets

        olap = olap_lats.copy()
        olap["timestamp"] = pd.to_datetime(olap["timestamp"]).dt.tz_localize(None)
        olap["offset"] = olap["timestamp"] - start_ts
        olap["offset_minute"] = olap["offset"].dt.total_seconds() / 60

        rel_bytes = bytes_scanned[olap_lats["query_idx"], 0]
        rel_mb = rel_bytes / 1000 / 1000
        rel_mb = np.clip(
            rel_mb, a_min=planner_config.athena_min_mb_per_query(), a_max=None
        )
        costs = planner_config.athena_usd_per_mb_scanned() * rel_mb
        olap["exec_cost"] = costs
        olap.loc[olap["engine"] != "athena", "exec_cost"] = 0.0

        return olap

    def print_olap_costs_per_region(
        self,
        olap_costs: List[Tuple[pd.DataFrame, bool]],
        reinterpret_second_as: float = 1.0,
    ) -> None:
        for idx, (offset_min, offset_max) in enumerate(self.blueprint_intervals):
            total_mins = offset_max - offset_min
            ratio = timedelta(days=30).total_seconds() / (
                total_mins * 60 * reinterpret_second_as
            )
            region_costs = 0.0
            for olap, is_repeating in olap_costs:
                rel = olap[
                    (olap["offset_minute"] >= offset_min)
                    & (olap["offset_minute"] <= offset_max)
                ]
                costs = rel["exec_cost"].sum()
                if is_repeating:
                    costs *= ratio
                region_costs += costs
            print(f"Region {idx} {str((offset_min, offset_max))}:")
            print("Monthly scan cost:", region_costs)
            print()


def _load_txn_data(
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


def _load_blueprints(
    data_dir: pathlib.Path,
) -> List[Tuple[datetime, ComparableBlueprint]]:
    def load_blueprint(filepath: pathlib.Path) -> Tuple[datetime, ComparableBlueprint]:
        filename_parts = filepath.name.split("_")
        date = filename_parts[-2]
        time = filename_parts[-1]
        time = time.split(".")[0]
        comb = "_".join([date, time])
        timestamp = datetime.strptime(comb, "%Y-%m-%d_%H-%M-%S")
        timestamp = timestamp.replace(tzinfo=None)

        with open(filepath, "rb") as file:
            bps = pickle.load(file)

        # The first blueprint is the one that was selected.
        return timestamp, bps[0]

    relevant = []
    for file in data_dir.iterdir():
        if file.name.startswith("final_") and file.name.endswith(".pkl"):
            relevant.append(file)

    data = [load_blueprint(file) for file in relevant]
    data.sort(key=lambda d: d[0])
    return data
