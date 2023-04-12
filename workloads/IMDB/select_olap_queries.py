import os
import numpy as np
from typing import Optional, Mapping
from workloads.IMDB.util import load_queries


def select_report_queries(
    query_dir: Optional[str] = None,
    queries: Optional[list[str]] = None,
    aurora_runtime: Optional[np.ndarray] = None,
    redshift_runtime: Optional[np.ndarray] = None,
    selected_query_rt_interval: Optional[Mapping[int, int]] = None,
    save_dir: Optional[str] = None,
    force: bool = False,
) -> list[str]:
    # select and fix a bunch of queries as the repeatedly executed reporting queries
    if save_dir:
        if os.path.exists(save_dir) and len(os.listdir(save_dir)) != 0:
            if not force:
                print(f"Reporting query already generated in folder: {save_dir}")
                return None
            else:
                for file in os.listdir(save_dir):
                    os.remove(os.path.join(save_dir, file))
        os.makedirs(save_dir, exist_ok=True)
    if queries is None or aurora_runtime is None or redshift_runtime is None:
        assert query_dir is not None
        queries, aurora_runtime, redshift_runtime = load_queries(query_dir)
    timeout = (
        np.infty
    )  # timeout is a value to assign to runtime if this execution leads to a timeout
    assert len(queries) == len(aurora_runtime) == len(redshift_runtime)

    if selected_query_rt_interval is None:
        selected_query_rt_interval = {1: 5, 10: 5, 50: 5, 200: 5}

    selected_idx = []
    total_aurora_rt = 0
    total_redshift_rt = 0

    low = 0
    for high in selected_query_rt_interval:
        idx = np.where(
            (aurora_runtime > low)
            & (aurora_runtime < high)
            & (redshift_runtime != timeout)
        )[0]
        num = selected_query_rt_interval[high]
        if len(idx) <= num:
            selected_idx.extend(list(idx))
        else:
            new_idx = np.random.choice(idx, size=num, replace=False)
            selected_idx.extend(list(new_idx))
        total_aurora_rt += np.sum(aurora_runtime[selected_idx])
        total_redshift_rt += np.sum(redshift_runtime[selected_idx])
        low = high
    print(
        f"selected reporting queries have {total_aurora_rt} sec Aurora runtime and {total_redshift_rt} "
        f"sec Redshift runtime"
    )

    if save_dir:
        for i in selected_idx:
            q = queries[i]
            with open(os.path.join(save_dir, f"{i}.sql"), "w+") as f2:
                f2.write(q)

    return [queries[i] for i in selected_idx]


def select_adhoc_queries(
    query_dir: Optional[str] = None,
    queries: Optional[list[str]] = None,
    aurora_runtime: Optional[np.ndarray] = None,
    redshift_runtime: Optional[np.ndarray] = None,
    num_query: int = 1,
    rt_interval: Optional[Mapping[int, int]] = None,
    aurora_timeout: bool = False,
    redshift_timeout: bool = False,
    save_dir: Optional[str] = None,
    return_query_idx: bool = True,
) -> list[str]:
    # randomly select {num_query} queries as the adhoc queries
    # num_query should reflect number of users at a given time
    if num_query == 0:
        return []
    if queries is None or aurora_runtime is None or redshift_runtime is None:
        assert query_dir is not None
        queries, aurora_runtime, redshift_runtime = load_queries(query_dir)
    timeout = (
        np.infty
    )  # timeout is a value to assign to runtime if this execution leads to a timeout

    if rt_interval is None:
        low_rt = 0
        high_rt = 1000000
    else:
        low_rt = rt_interval[0]
        high_rt = rt_interval[1]

    if aurora_timeout:
        if redshift_timeout:
            idx = np.where((aurora_runtime == timeout) & (redshift_runtime == timeout))[
                0
            ]
        else:
            idx = np.where((aurora_runtime == timeout) & (redshift_runtime != timeout))[
                0
            ]
    elif redshift_timeout:
        idx = np.where(
            (aurora_runtime > low_rt)
            & (aurora_runtime < high_rt)
            & (redshift_runtime == timeout)
        )[0]
    else:
        idx = np.where(
            (aurora_runtime > low_rt)
            & (aurora_runtime < high_rt)
            & (redshift_runtime > low_rt)
            & (redshift_runtime < high_rt)
        )[0]

    if len(idx) < num_query:
        selected_idx = idx
        print(
            f"WARNING: requesting {num_query} queries but only {len(idx)} queries available in the "
            f"runtime range {low_rt} to {high_rt}"
        )
    else:
        selected_idx = np.random.choice(idx, size=num_query, replace=False)

    if save_dir:
        for i in selected_idx:
            q = queries[i]
            with open(os.path.join(save_dir, f"{i}.sql"), "w+") as f2:
                f2.write(q)
    if return_query_idx:
        return [(i, queries[i]) for i in selected_idx]
    return [queries[i] for i in selected_idx]
