import json
import os
import re
import shutil
import time
from json.decoder import JSONDecodeError
from tqdm import tqdm

from workloads.cross_db_benchmark.benchmark_tools.load_database import create_db_conn
from workloads.cross_db_benchmark.benchmark_tools.utils import (
    load_json,
    compute_workload_splits,
)

column_regex = re.compile('"(\S+)"."(\S+)"')


def run_athena_workload(
    workload_path,
    database,
    db_name,
    target_path,
    run_kwargs,
    timeout_sec,
    cap_workload,
    rank,
    world_size,
):
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    db_conn = create_db_conn(database, db_name, None, None)

    with open(workload_path) as f:
        content = f.readlines()
    queries = [x.strip() for x in content]

    start_offset, end_offset = compute_workload_splits(len(queries), rank, world_size)
    print("----------------------------------")
    print("Rank:", rank)
    print("World size:", world_size)
    print("Running queries in range: [%d, %d)".format(start_offset, end_offset))
    print("----------------------------------")
    relevant_queries = queries[start_offset:end_offset]

    # extract column statistics: do Athena support this? Probably not
    # database_stats = db_conn.collect_db_statistics()

    # check if workload already exists
    query_list = []
    seen_queries = set()
    time_offset = 0
    if os.path.exists(target_path):
        try:
            last_run = load_json(target_path, namespace=False)
            query_list = last_run["query_list"]
            if "total_time_secs" in last_run:
                time_offset = last_run["total_time_secs"]
            for q in query_list:
                seen_queries.add(q["sql"])
        except JSONDecodeError:
            print("Could not read json")

    # extract query plans
    start_t = time.perf_counter()
    valid_queries = 0
    for i, sql_query in enumerate(tqdm(relevant_queries)):
        if cap_workload and i >= cap_workload:
            break

        curr_statistics = db_conn.run_query_collect_statistics(sql_query, timeout_sec)
        curr_statistics.update(sql=sql_query)
        query_list.append(curr_statistics)

        run_stats = dict(
            query_list=query_list,
            run_kwargs=run_kwargs,
            total_time_secs=time_offset + (time.perf_counter() - start_t),
        )

        # save to json
        # write to temporary path and then move
        if len(query_list) % 50 == 0:
            save_workload(run_stats, target_path)

    run_stats = dict(
        query_list=query_list,
        run_kwargs=run_kwargs,
        total_time_secs=time_offset + (time.perf_counter() - start_t),
    )
    save_workload(run_stats, target_path)

    print(
        f"Executed workload {workload_path} in {time_offset + time.perf_counter() - start_t:.2f} secs"
    )


def save_workload(run_stats, target_path):
    target_temp_path = os.path.join(
        os.path.dirname(target_path), f"{os.path.basename(target_path)}_temp"
    )
    with open(target_temp_path, "w") as outfile:
        json.dump(run_stats, outfile)
    shutil.move(target_temp_path, target_path)
