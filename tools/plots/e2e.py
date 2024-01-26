import pathlib
import pickle
import numpy as np
import numpy.typing as npt
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.figure as plt_fig
from matplotlib.gridspec import GridSpec
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Literal, Callable

from brad.config.planner import PlannerConfig
from brad.planner.compare.blueprint import ComparableBlueprint
from brad.planner.beam.query_based_candidate import BlueprintCandidate as QbCandidate
from brad.planner.beam.table_based_candidate import BlueprintCandidate as TbCandidate
from brad.planner.beam.fpqb_candidate import BlueprintCandidate as FpqbCandidate


class RecordedRun:
    @classmethod
    def load(cls, exp_dir: str, is_trace: bool = False) -> "RecordedRun":
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
            if oltp_stats is not None:
                stats.append(oltp_stats)
            if oltp_ind_lats is not None:
                ind_lats.append(oltp_ind_lats)

        if len(stats) > 0:
            oltp_stats = pd.concat(stats).sort_values(by=["num_clients"])
        else:
            oltp_stats = pd.DataFrame.empty
        oltp_ind_lats = pd.concat(ind_lats)

        all_olap = []

        if not is_trace:
            for inner in base.iterdir():
                if not inner.is_dir() or not inner.name.startswith("ra_"):
                    continue
                if inner.name == "ra_vector":
                    clients = 2
                else:
                    name_parts = inner.name.split("_")
                    if name_parts[1] == "sweep":
                        clients = int(name_parts[2])
                    else:
                        clients = int(name_parts[1])
                for c in range(clients):
                    try:
                        olap_inner = pd.read_csv(
                            inner / "repeating_olap_batch_{}.csv".format(c)
                        )
                        olap_inner["timestamp"] = pd.to_datetime(
                            olap_inner["timestamp"], format="mixed"
                        )
                        olap_inner["timestamp"] = olap_inner[
                            "timestamp"
                        ].dt.tz_localize(None)
                        olap_inner.insert(0, "num_clients", clients)
                        olap_inner.insert(1, "qtype", "repeating")
                        all_olap.append(olap_inner)
                    except pd.errors.EmptyDataError:
                        pass

            for inner in base.iterdir():
                if not inner.is_dir() or not inner.name.startswith("adhoc"):
                    continue
                num_clients = 0
                this_adhoc = []
                for data_file in inner.iterdir():
                    if not data_file.name.endswith(".csv"):
                        continue
                    num_clients += 1
                    olap_inner = pd.read_csv(data_file)
                    olap_inner["timestamp"] = pd.to_datetime(
                        olap_inner["timestamp"], format="mixed"
                    )
                    olap_inner["timestamp"] = olap_inner["timestamp"].dt.tz_localize(
                        None
                    )
                    this_adhoc.append(olap_inner)
                adhoc = pd.concat(this_adhoc)
                adhoc.insert(0, "num_clients", num_clients)
                adhoc.insert(1, "qtype", "adhoc")
                all_olap.append(adhoc)

        for inner in base.iterdir():
            if not inner.is_dir() or not inner.name.startswith("trace"):
                continue
            for datafile in inner.iterdir():
                if not (
                    datafile.name.startswith("trace_client")
                    and datafile.name.endswith(".csv")
                ):
                    continue
                df = pd.read_csv(datafile)
                df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed")
                df["timestamp"] = df["timestamp"].dt.tz_localize(None)
                df.insert(0, "num_clients", 10)
                df.insert(1, "qtype", "trace")
                all_olap.append(df)
                print("Adding", datafile.name, "Rows", len(df))

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

        if (base / "cost_metrics.csv").exists():
            baseline_costs = _load_process_costs(base / "cost_metrics.csv")
        else:
            baseline_costs = None

        return cls(
            oltp_ind_lats,
            olap,
            oltp_stats,
            txn_thpt,
            events,
            blueprints,
            baseline_costs,
        )

    def __init__(
        self,
        txn_lats: pd.DataFrame,
        olap_lats: pd.DataFrame,
        txn_stats: pd.DataFrame,
        txn_thpt: Optional[pd.DataFrame],
        events: Optional[pd.DataFrame],
        blueprints: List[Tuple[datetime, ComparableBlueprint]],
        baseline_costs: Optional[pd.DataFrame],
    ) -> None:
        self.txn_lats = txn_lats
        self.olap_lats = olap_lats
        self.txn_stats = txn_stats
        self.txn_thpt = txn_thpt
        self.events = events
        self.blueprints = blueprints
        self.baseline_costs = baseline_costs

        self._txn_lat_p90: Optional[pd.DataFrame] = None
        self._ana_lat_p90: Optional[pd.DataFrame] = None
        self._timestamp_offsets: Optional[Tuple[pd.Timestamp, pd.Timestamp]] = None
        self._blueprint_intervals: Optional[List[Tuple[float, float]]] = None
        self._events_with_offset: Optional[pd.DataFrame] = None

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
    def txn_lat_p90_5min(self) -> pd.DataFrame:
        return self._agg_txn_lats_window(window_str="5min", quantile=0.9)

    @property
    def ana_lat_p90(self) -> pd.DataFrame:
        if self._ana_lat_p90 is not None:
            return self._ana_lat_p90
        self._ana_lat_p90 = self._agg_ana_lats(window_str="5min", quantile=0.9)
        return self._ana_lat_p90

    @property
    def ana_lat_p90_4min(self) -> pd.DataFrame:
        return self._agg_ana_lats(window_str="4min", quantile=0.9)

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

    @property
    def events_with_offset(self) -> Optional[pd.DataFrame]:
        if self._events_with_offset is not None:
            return self._events_with_offset
        if self.events is None:
            return None
        self._events_with_offset = self.events.copy()
        start_ts, _ = self.timestamp_offsets
        self._events_with_offset["offset"] = (
            self._events_with_offset["timestamp"] - start_ts
        )
        self._events_with_offset["offset_minute"] = (
            self._events_with_offset["offset"].dt.total_seconds() / 60.0
        )
        return self._events_with_offset

    def txn_lat_p90_rolling(self, window_mins=5) -> pd.DataFrame:
        return self.txn_lat_p90.rolling(window_mins, min_periods=1).mean()

    def ana_lat_p90_rolling(self, window_mins=5) -> pd.DataFrame:
        return self.ana_lat_p90.rolling(window_mins, min_periods=1).mean()

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
            if hasattr(bp, "get_operational_monetary_cost_without_scans"):
                operational_cost = bp.get_operational_monetary_cost_without_scans()
            else:
                # This is to help with serialized legacy versions of the object.
                # Not ideal, but this is the best we can do.
                if isinstance(bp, QbCandidate) or isinstance(bp, TbCandidate):
                    operational_cost = bp.storage_cost + bp.provisioning_cost
                elif isinstance(bp, FpqbCandidate):
                    operational_cost = (
                        bp.score.storage_cost + bp.score.provisioning_cost
                    )
                else:
                    operational_cost = np.nan
            print("Operational cost excluding Athena scans ($/hr):", operational_cost)
            print()

    def _agg_txn_lats(self, quantile: float) -> pd.DataFrame:
        rel = self.txn_lats[["timestamp", "run_time_s"]].copy()
        rel["timestamp"] = pd.to_datetime(rel["timestamp"], format="mixed")
        rel = rel.sort_values(by=["timestamp"])
        rel = rel.set_index("timestamp")
        rel = rel.resample("1T").quantile(quantile).reset_index()
        return rel

    def _agg_txn_lats_window(
        self, window_str: str, quantile: float, fill_missing: Optional[float] = None
    ) -> pd.DataFrame:
        rel = self.txn_lats[["timestamp", "run_time_s"]].copy()
        rel["timestamp"] = pd.to_datetime(rel["timestamp"])
        rel["run_time_s"] = pd.to_numeric(rel["run_time_s"])
        rel = rel.sort_values(by=["timestamp"])
        rel = rel.set_index("timestamp")
        over_window = rel.rolling(window_str, min_periods=1).quantile(quantile)
        over_window = over_window.resample("1T").max()
        if fill_missing is not None:
            over_window = over_window.fillna(fill_missing)
        over_window = over_window.reset_index()
        return over_window

    def _agg_ana_lats(
        self, window_str: str, quantile: float, fill_missing: Optional[float] = None
    ) -> pd.DataFrame:
        rel = self.olap_lats[["timestamp", "run_time_s"]].copy()
        rel["timestamp"] = pd.to_datetime(rel["timestamp"])
        rel["run_time_s"] = pd.to_numeric(rel["run_time_s"])
        rel = rel.sort_values(by=["timestamp"])
        rel = rel.set_index("timestamp")
        over_window = rel.rolling(window_str, min_periods=1).quantile(quantile)
        over_window = over_window.resample("1T").max()
        if fill_missing is not None:
            over_window = over_window.fillna(fill_missing)
        over_window = over_window.reset_index()
        return over_window

    def _compute_timestamp_offsets(self) -> Tuple[pd.Timestamp, pd.Timestamp]:
        if self.txn_thpt is not None:
            rel = self.txn_thpt.loc[self.txn_thpt["txn_end_per_s"] > 0]
            start_ts = rel.iloc[0]["timestamp"]
            end_ts = rel.iloc[-1]["timestamp"]
        else:
            print("Using txn latency to establish timestamp offset")
            start_ts = self.txn_lats.iloc[0]["timestamp"]
            end_ts = self.txn_lats.iloc[-1]["timestamp"]

        parsed_start, parsed_end = (
            pd.to_datetime(start_ts).tz_localize(None),
            pd.to_datetime(end_ts).tz_localize(None),
        )

        # Adjust the end timestamp if needed.
        olap_ts = self.olap_lats["timestamp"]
        parsed_end = max(parsed_end, olap_ts.max())
        return parsed_start, parsed_end

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
        end_offset_minute = (end_ts - start_ts).total_seconds() / 60.0
        if last_offset is not None:
            intervals.append((last_offset, end_offset_minute))
        else:
            intervals.append((0.0, end_offset_minute))
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


def get_e2e_axes(
    size: Literal["small", "large"],
    txn_ceiling_ms=30.0,
    ana_ceiling_s=30.0,
    custom_subplots: Optional[
        Callable[[GridSpec], Tuple[plt.Axes, plt.Axes, plt.Axes]]
    ] = None,
) -> Tuple[plt_fig.Figure, plt.Axes, plt.Axes, plt.Axes]:
    fig = plt.figure(
        figsize=(7, 5) if size == "small" else (10, 6),
        # This option is problematic when also using `align_ylabels()`.
        # Use `savefig("...", bbox_inches="tight")` instead.
        # tight_layout=True,
    )
    grid = GridSpec(nrows=3, ncols=1, height_ratios=[2, 2, 1], hspace=0.45)

    if custom_subplots is not None:
        txn_ax, ana_ax, cst_ax = custom_subplots(grid)
    else:
        txn_ax = plt.subplot(grid[0, 0])
        ana_ax = plt.subplot(grid[1, 0], sharex=txn_ax)
        cst_ax = plt.subplot(grid[2, 0], sharex=ana_ax)
        fig.align_ylabels()

    # Transaction Latency ceiling
    txn_ax.axhspan(ymin=0, ymax=txn_ceiling_ms, color="#000", alpha=0.05)
    txn_ax.axhline(y=txn_ceiling_ms, color="#000", alpha=0.5, lw=1.5)

    # OLAP Latency ceiling
    ana_ax.axhspan(ymin=-5, ymax=ana_ceiling_s, color="#000", alpha=0.05)
    ana_ax.axhline(y=ana_ceiling_s, color="#000", alpha=0.5, lw=1.5)

    cst_ax.set_ylabel("Monthly\nCost ($)")
    cst_ax.set_xlabel("Time Elapsed (minutes)")
    txn_ax.set_ylabel("Transaction\nLatency (ms)")
    ana_ax.set_ylabel("Analytics\nLatency (s)")

    return fig, txn_ax, ana_ax, cst_ax


def get_e2e_axes_with_workload(
    size: Literal["small", "large"],
    txn_ceiling_ms=30.0,
    ana_ceiling_s=30.0,
    custom_subplots: Optional[
        Callable[[GridSpec], Tuple[plt.Axes, plt.Axes, plt.Axes, plt.Axes]]
    ] = None,
) -> Tuple[plt_fig.Figure, plt.Axes, plt.Axes, plt.Axes, plt.Axes]:
    fig = plt.figure(
        figsize=(7, 6) if size == "small" else (10, 7),
        # This option is problematic when also using `align_ylabels()`.
        # Use `savefig("...", bbox_inches="tight")` instead.
        # tight_layout=True,
    )
    grid = GridSpec(nrows=4, ncols=1, height_ratios=[2, 2, 1, 1], hspace=0.45)

    if custom_subplots is not None:
        txn_ax, ana_ax, cst_ax, wrk_ax = custom_subplots(grid)
    else:
        txn_ax = plt.subplot(grid[0, 0])
        ana_ax = plt.subplot(grid[1, 0], sharex=txn_ax)
        cst_ax = plt.subplot(grid[2, 0], sharex=ana_ax)
        wrk_ax = plt.subplot(grid[3, 0], sharex=cst_ax)
        fig.align_ylabels()

    # Transaction Latency ceiling
    txn_ax.axhspan(ymin=0, ymax=txn_ceiling_ms, color="#000", alpha=0.05)
    txn_ax.axhline(y=txn_ceiling_ms, color="#000", alpha=0.5, lw=1.5)

    # OLAP Latency ceiling
    ana_ax.axhspan(ymin=-5, ymax=ana_ceiling_s, color="#000", alpha=0.05)
    ana_ax.axhline(y=ana_ceiling_s, color="#000", alpha=0.5, lw=1.5)

    cst_ax.set_ylabel("Monthly\nCost ($)")
    txn_ax.set_ylabel("Transaction\nLatency (ms)")
    ana_ax.set_ylabel("Analytics\nLatency (s)")
    wrk_ax.set_xlabel("Time Elapsed (minutes)")

    return fig, txn_ax, ana_ax, cst_ax, wrk_ax


def plot_brad_event(
    axes: List[plt.Axes], events: pd.DataFrame, event_name: str, linestyle: str
) -> None:
    rel = events[events["event"] == event_name]
    for ts in rel["offset_minute"]:
        for ax in axes:
            ax.axvline(x=ts, color="#333", linestyle=linestyle, linewidth=1.5)


def assemble_brad_cost_data(
    hourly_cost_per_region: List[float], regions: List[Tuple[float, float]]
) -> Tuple[npt.NDArray, npt.NDArray]:
    assert len(hourly_cost_per_region) == len(regions)
    x_segments = []
    val_segments = []

    for hourly_cost, (start, end) in zip(hourly_cost_per_region, regions):
        xs = np.linspace(start, end)
        vals = np.ones_like(xs)
        vals *= hourly_cost * 24 * 30
        x_segments.append(xs)
        val_segments.append(vals)

    xs_full = np.concatenate(x_segments)
    vals_full = np.concatenate(val_segments)
    return xs_full, vals_full


def compute_cumulative_cost(minutes: npt.NDArray, monthly_costs: npt.NDArray) -> float:
    minute_cost = monthly_costs / 30 / 24 / 60
    return np.trapz(minute_cost, x=minutes).item()


def _load_txn_data(
    data_dir: pathlib.Path, num_clients: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    all_lats = []
    all_stats = []

    for exp_file in data_dir.iterdir():
        try:
            if exp_file.name.startswith("oltp_latency_"):
                df = pd.read_csv(exp_file)
                df.pop("txn_idx")
                df.insert(0, "num_clients", num_clients)
                all_lats.append(df)

            elif exp_file.name.startswith("oltp_stats_"):
                stats_df = pd.read_csv(exp_file)
                stats_df = stats_df.pivot_table(
                    index=None, columns="stat", values="value"
                )
                stats_df.index.name = None
                stats_df.insert(0, "num_clients", num_clients)
                all_stats.append(stats_df)
        except pd.errors.EmptyDataError:
            pass

    if len(all_lats) > 0:
        comb_lats = pd.concat(all_lats, ignore_index=True)
    else:
        comb_lats = None

    if len(all_stats) > 0:
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
    else:
        comb_stats = None

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


def _load_process_costs(cost_file: pathlib.Path) -> pd.DataFrame:
    df = pd.read_csv(cost_file)
    aurora = df[df["engine"] == "Aurora"][["timestamp", "cost"]]
    redshift = df[df["engine"] == "Redshift"][["timestamp", "cost"]]
    comb = pd.merge(
        aurora, redshift, how="outer", on="timestamp", suffixes=("_aurora", "_redshift")
    )
    comb = comb.sort_values(by=["timestamp"], ascending=True, ignore_index=True)
    comb = comb.fillna(0.0)
    comb["total_cost_per_minute"] = comb["cost_aurora"] + comb["cost_redshift"]
    # 30 days in a month.
    comb["total_cost_per_month"] = comb["total_cost_per_minute"] * 60 * 24 * 30
    comb["aurora_cost_per_month"] = comb["cost_aurora"] * 60 * 24 * 30
    return comb
