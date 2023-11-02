import numpy as np
import matplotlib.pyplot as plt
import os
import pandas as pd
from typing import Optional, List, Tuple
import numpy.typing as npt


def convert_str_to_minute(time_of_day: str) -> int:
    h = int(time_of_day.split(":")[0])
    m = int(time_of_day.split(":")[-1])
    return h * 60 + m


def convert_minute_to_str(time_of_day: int) -> str:
    hour = time_of_day // 60
    assert hour < 24
    minute = time_of_day % 60
    hour_str = str(hour) if hour >= 10 else "0" + str(hour)
    minute_str = str(minute) if minute >= 10 else "0" + str(minute)
    return f"{hour_str}:{minute_str}"


def load_all_log_csv(log_dir: str) -> pd.DataFrame:
    all_df = []
    for file in os.listdir(log_dir):
        if file.startswith("repeating_olap_batch_") and file.endswith(".csv"):
            all_df.append(pd.read_csv(os.path.join(log_dir, file)))
    df = pd.concat(all_df)
    return df


def plot_overall_runtime_hist(
    log_dir: str, nbins: int = 100, bins: Optional[npt.NDArray] = None
) -> Tuple[npt.NDArray, npt.NDArray]:
    df = load_all_log_csv(log_dir)
    runtime = df["run_time_s"].values
    if bins is None:
        count, bin_breaks, _ = plt.hist(runtime, bins=nbins)
    else:
        count, bin_breaks, _ = plt.hist(runtime, bins=bins)
    plt.xlabel("Query runtime")
    plt.ylabel("Number of queries")
    plt.show()
    return count, bin_breaks


def plot_num_queries_with_time(
    log_dir: str, time_interval: int = 15
) -> Tuple[List[str], List[int]]:
    df = load_all_log_csv(log_dir)
    time_of_day = df["time_of_day"].values
    time_of_day = np.asarray([convert_str_to_minute(i) for i in time_of_day])
    time_str = []
    count = []
    start = 0
    for h in range(24):
        for m in range(0, 60, time_interval):
            end = h * 60 + m
            if end == 0:
                continue
            time_str.append(convert_minute_to_str(end))
            count.append(int(np.sum((time_of_day >= start) & (time_of_day < end))))
            start = end
    plt.bar(np.arange(len(count)), count)
    plt.xlabel("Time of the day")
    plt.ylabel("Number of queries")
    plt.show()
    return time_str, count


def plot_num_concurrent_queries_hist(log_dir: str) -> Tuple[List[int], List[int]]:
    df = load_all_log_csv(log_dir)
    runtime = df["run_time_s"].values
    end = df["time_since_execution"].values
    start = end - runtime
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

    num_concurrent_queries = []
    count = []
    for num_query in range(int(np.max(res)) + 1):
        num_concurrent_queries.append(num_query)
        count.append(int(np.sum(res == num_query)))
    plt.bar(np.arange(len(count)), count)
    plt.xlabel("Number of concurrent queries")
    plt.ylabel("Count")
    plt.show()
    return num_concurrent_queries, count
