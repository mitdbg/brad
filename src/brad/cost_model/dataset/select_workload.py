import os
import numpy as np
import json
from workloads.cross_db_benchmark.benchmark_tools.utils import load_json, dumper


sample_dist = {1000: 0.2, 5000: 0.2, 10000: 0.2, 50000: 0.3, 1000000: 0.1}


def combine_workload(workload_runs, keep_join=1):
    combined_run = {
        "parsed_plans": [],
        "parsed_queries": [],
        "sql_queries": [],
        "database_stats": None,
        "run_kwargs": None,
    }
    runtimes = []
    for workload in workload_runs:
        run = load_json(workload, namespace=False)
        if combined_run["database_stats"] is None:
            combined_run["database_stats"] = run["database_stats"]
        if combined_run["run_kwargs"] is None:
            combined_run["run_kwargs"] = run["run_kwargs"]
        for i, q in enumerate(run["parsed_queries"]):
            if len(q["join_nodes"]) >= keep_join:
                combined_run["parsed_queries"].append(q)
                combined_run["parsed_plans"].append(run["parsed_plans"][i])
                combined_run["sql_queries"].append(run["sql_queries"][i])
                runtimes.append(q["plan_runtime"])
    return combined_run, runtimes


def select_workload(
    workload_runs,
    target,
    seed=0,
    keep_join=1,
    num_train=400,
    num_test=100,
    get_debug=False,
):
    # randomly select a training and testing workload from the existing workload, making sure they cover a
    # diverse runtime range
    np.random.seed(seed)
    run, runtimes = combine_workload(workload_runs, keep_join)
    selected_train_idx = []
    selected_test_idx = []
    runtimes = np.asarray(runtimes)
    train_ratio = num_train / (num_train + num_test)

    min_rt_interval = 0
    for max_rt_interval in sample_dist:
        n_train = int(num_train * sample_dist[max_rt_interval])
        n_test = int(num_test * sample_dist[max_rt_interval])
        idx = np.where((runtimes > min_rt_interval) & (runtimes <= max_rt_interval))[0]
        if len(idx) <= n_train + n_test:
            train_idx = idx[: int(len(idx) * train_ratio)]
            test_idx = idx[int(len(idx) * train_ratio) :]
        else:
            selected_idx = np.random.choice(idx, size=n_train + n_test, replace=False)
            train_idx = selected_idx[:n_train]
            test_idx = selected_idx[n_train:]
        print(
            f"Selected {len(train_idx)} training queries with {min_rt_interval}ms to {max_rt_interval}ms"
        )
        print(
            f"Selected {len(test_idx)} testing queries with {min_rt_interval}ms to {max_rt_interval}ms"
        )
        selected_train_idx.extend(train_idx)
        selected_test_idx.extend(test_idx)
        min_rt_interval = max_rt_interval

    print(
        f"Training query total runtime is {np.sum(runtimes[selected_train_idx])/1000}s"
    )
    print(f"Testing query total runtime is {np.sum(runtimes[selected_test_idx])/1000}s")
    train_path = os.path.join(target, "train")
    test_path = os.path.join(target, "test")
    os.makedirs(train_path, exist_ok=True)
    os.makedirs(test_path, exist_ok=True)

    if get_debug:
        debug_idx = np.asarray(selected_train_idx)[
            np.linspace(10, int(len(selected_train_idx) / 4), 8, dtype=int)
        ]
        print(f"Debug query total runtime is {np.sum(runtimes[debug_idx]) / 1000}s")
        debug_path = os.path.join(target, "debug")
        os.makedirs(debug_path, exist_ok=True)

    selected_run = {
        "parsed_plans": [],
        "parsed_queries": [],
        "sql_queries": [],
        "database_stats": run["database_stats"],
        "run_kwargs": run["run_kwargs"],
    }

    for i, idx in enumerate(selected_train_idx):
        selected_run["parsed_queries"].append(run["parsed_queries"][idx])
        selected_run["parsed_plans"].append(run["parsed_plans"][idx])
        selected_run["sql_queries"].append(run["sql_queries"][idx])
        file = f"{i}.sql"
        with open(os.path.join(train_path, file), "w+") as f:
            f.write(run["sql_queries"][idx])
        if get_debug and idx in debug_idx:
            with open(os.path.join(debug_path, file), "w+") as f:
                f.write(run["sql_queries"][idx])

    for i, idx in enumerate(selected_test_idx):
        selected_run["parsed_queries"].append(run["parsed_queries"][idx])
        selected_run["parsed_plans"].append(run["parsed_plans"][idx])
        selected_run["sql_queries"].append(run["sql_queries"][idx])
        file = f"{i + len(selected_train_idx)}.sql"
        with open(os.path.join(test_path, file), "w+") as f:
            f.write(run["sql_queries"][idx])

    with open(os.path.join(target, "selected_run.json"), "w") as outfile:
        json.dump(selected_run, outfile, default=dumper)


def print_workload_stats(workload_runs, keep_join=1):
    run, runtimes = combine_workload(workload_runs, keep_join)
    runtimes = np.asarray(runtimes)
    print(f"number of parsed_queries: {len(run['parsed_queries'])}")

    min_rt_interval = 0
    for max_rt_interval in sample_dist:
        idx = np.where((runtimes > min_rt_interval) & (runtimes <= max_rt_interval))[0]
        print(
            f"{len(idx)} number of queries found with runtime between {min_rt_interval}ms and {max_rt_interval}ms"
        )
        min_rt_interval = max_rt_interval
