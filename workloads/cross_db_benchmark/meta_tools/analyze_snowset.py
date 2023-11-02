import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import List
import numpy.typing as npt


def get_column_names(csv_file_name: str) -> List[str]:
    column_names = pd.read_csv(csv_file_name, sep=",", header=0, skiprows=0, nrows=0)
    column_names = list(column_names.columns)
    return column_names


def create_sample(
    file_name: str, sample_size: float = 0.01, nrows: int = 100000
) -> pd.DataFrame:
    # create a uniform sample of the table
    sample_df = []
    column_names = pd.read_csv(file_name, sep=",", header=0, skiprows=0, nrows=0)
    column_names = list(column_names.columns)
    skiprows = 0
    while True:
        try:
            df = pd.read_csv(
                file_name, sep=",", header=None, skiprows=skiprows, nrows=nrows
            )
        except:
            break
        sample_df.append(df)
        skiprows += int(nrows / sample_size)
    sample_df = pd.concat(sample_df)
    sample_df.columns = column_names
    return sample_df


def load_parquet(parquet_file_name: str, columns: List[str]) -> pd.DataFrame:
    df = pd.read_parquet(parquet_file_name, columns=columns)
    return df


def get_runtime_distribution(
    df: pd.DataFrame, max_runtime: float
) -> (npt.NDArray, npt.NDArray):
    runtime = df["durationTotal"].values / 1000
    density, bins = plt.hist(runtime[runtime < max_runtime], bins=100)
    return density, bins


def get_num_concurrent_queries(rows: pd.DataFrame) -> npt.NDArray:
    start = rows["createdTime"].values
    end = rows["endTime"].values
    idx = np.argsort(start)
    start = start[idx]
    end = end[idx]

    res = np.zeros(len(start))
    for i in range(len(start)):
        s = start[i]
        e = end[i]
        ne = np.searchsorted(
            start[i + 1 :], e
        )  # number of queries start after s and before e
        ns = np.sum(end[:i] > s)  # number of queries start before s and ends after s
        res[i] = ne + ns
    return res


def get_num_queries_per_time_interval(
    rows: pd.DataFrame,
    dates: List[str] = None,
    time_gap: int = 10,
    day_type: str = "all",
    aggregate: bool = True,
) -> (List, npt.NDArray, npt.NDArray):
    """
    Get the number of queries per time interval of the given dates
    time_gap: provide aggregated stats every {time_interval} minutes, for current implementation
            please make it a number divisible by 60
    day_type: choose between "all", "weekday", "weekend"
    aggregate: average across days?
    """
    assert day_type in ["all", "weekday", "weekend"], f"invalid day_type: {day_type}"
    if dates is None:
        dates = [
            "2018-02-22",
            "2018-02-23",
            "2018-02-24",
            "2018-02-25",
            "2018-02-26",
            "2018-02-27",
            "2018-02-28",
            "2018-03-01",
            "2018-03-02",
            "2018-03-03",
            "2018-03-04",
            "2018-03-05",
            "2018-03-06",
        ]
    if day_type == "weekday":
        dates = [
            d
            for d in dates
            if d not in ["2018-02-24", "2018-02-25", "2018-03-03", "2018-03-04"]
        ]
    elif day_type == "weekend":
        dates = [
            d
            for d in dates
            if d in ["2018-02-24", "2018-02-25", "2018-03-03", "2018-03-04"]
        ]

    time_intervals = []
    for h in range(24):
        hour = str(h) if h >= 10 else f"0{h}"
        for m in range(0, 60, time_gap):
            minute = str(m) if m >= 10 else f"0{m}"
            time_intervals.append(f"{hour}:{minute}:00")

    start_time = rows["createdTime"].values
    end = rows["endTime"].values
    idx = np.argsort(start_time)
    start_time = start_time[idx]
    end_time = end[idx]

    num_queries = []
    num_concurrent_queries = []
    all_time_interval = []
    outer_start = 0  # the start index of outer loop
    for date in dates:
        outer_end = np.array([f"{date}T23:59:59"], dtype="datetime64[ns]")
        outer_end = np.searchsorted(start_time, outer_end[0])
        curr_time_intervals = np.array(
            [f"{date}T{t}" for t in time_intervals], dtype="datetime64[ns]"
        )
        if not aggregate:
            all_time_interval.extend(curr_time_intervals)
        if outer_end - outer_start == 0:
            num_queries.append(np.zeros(len(time_intervals)))
            num_concurrent_queries.append(np.zeros(len(time_intervals)))
            continue

        curr_start_time = start_time[outer_start:outer_end]
        curr_end_time = end_time[outer_start:outer_end]

        curr_num_queries = []
        curr_num_concurrent_queries = []
        inner_start = 0  # the start index of inner loop
        for t in curr_time_intervals:
            inner_end = np.searchsorted(curr_start_time, t)
            if inner_end - inner_start == 0:
                curr_num_queries.append(0)
                curr_num_concurrent_queries.append(0)
                continue
            curr_num_queries.append(inner_end - inner_start)
            num_concurrent_queries_cnt = 0
            for i in range(inner_start, inner_end):
                s = curr_start_time[i]
                e = curr_end_time[i]
                if i < len(curr_start_time):
                    ne = np.searchsorted(
                        curr_start_time[i + 1 :], e
                    )  # number of queries start after s and before e
                else:
                    ne = 0
                ns = np.sum(
                    curr_end_time[:i] > s
                )  # number of queries start before s and ends after s
                num_concurrent_queries_cnt += ne + ns
            curr_num_concurrent_queries.append(
                num_concurrent_queries_cnt / (inner_end - inner_start)
            )
            inner_start = inner_end
        num_queries.append(np.asarray(curr_num_queries))
        num_concurrent_queries.append(np.asarray(curr_num_concurrent_queries))
        outer_start = outer_end

    if aggregate:
        return (
            time_intervals,
            np.mean(num_queries, axis=0),
            np.mean(num_concurrent_queries, axis=0),
        )
    else:
        return (
            all_time_interval,
            np.concatenate(num_queries),
            np.concatenate(num_concurrent_queries),
        )


def aggregate_across_database(
    parquet_file_name: str,
    min_num_queries: int = 5000,
    num_db_cap: int = 100,
    dates: List[str] = None,
    time_gap: int = 10,
    day_type: str = "all",
) -> (List, npt.NDArray, npt.NDArray):
    """
    Get the number of queries per time interval of the given dates (averaged across all database for all days)
    min_num_queries: discard database with fewer than {min_num_queries} queries
    num_db_cap: only consider the first {num_db_cap} databases
    time_gap: provide aggregated stats every {time_interval} minutes, for current implementation
            please make it a number divisible by 60
    day_type: choose between "all", "weekday", "weekend"
    """
    df = load_parquet(
        parquet_file_name, ["databaseId", "durationTotal", "createdTime", "endTime"]
    )
    num_db = 0
    agg_num_queries = []
    agg_num_concurrent_queries = []
    all_time_interval = None
    for db, rows in df.groupby("databaseId"):
        if len(rows) > min_num_queries:
            (
                all_time_interval,
                num_queries,
                num_concurrent_queries,
            ) = get_num_queries_per_time_interval(rows, dates, time_gap, day_type)
            agg_num_queries.append(num_queries)
            agg_num_concurrent_queries.append(num_concurrent_queries)
            num_db += 1
            if num_db >= num_db_cap:
                break
    return (
        all_time_interval,
        np.mean(agg_num_queries, axis=0),
        np.mean(agg_num_concurrent_queries, axis=0),
    )


def num_concurrent_queries_across_database(
    parquet_file_name: str, min_num_queries: int = 5000, num_db_cap: int = 100
) -> (List, npt.NDArray, npt.NDArray):
    """
    Get the number of concurrent queries
    min_num_queries: discard database with fewer than {min_num_queries} queries
    num_db_cap: only consider the first {num_db_cap} databases
    """
    df = load_parquet(
        parquet_file_name, ["databaseId", "durationTotal", "createdTime", "endTime"]
    )
    num_db = 0
    total_num_concurrent_queries = []
    avg_num_query_per_day = []
    for db, rows in df.groupby("databaseId"):
        if len(rows) > min_num_queries:
            avg_num_query_per_day.append(len(rows) / 13)  # 13 days of data in snowset
            num_concurrent_queries = get_num_concurrent_queries(rows)
            total_num_concurrent_queries.append(num_concurrent_queries)
            num_db += 1
            if num_db >= num_db_cap:
                break
    return np.concatenate(total_num_concurrent_queries), np.mean(avg_num_query_per_day)
