import json
import os
import random
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
from workloads.cross_db_benchmark.benchmark_tools.database import DatabaseSystem

column_regex = re.compile('"(\S+)"."(\S+)"')


def extract_columns(sql):
    return [m for m in column_regex.findall(sql)]


def index_creation_deletion(existing_indexes, sql_part, db_conn, timeout_sec):
    cols = extract_columns(sql_part)
    index_cols = set(existing_indexes.keys())
    no_idxs = len(index_cols.intersection(cols))

    if len(cols) > 0:
        # not a single index for sql available, create one
        if no_idxs == 0:
            t, c = random.choice(cols)
            db_conn.set_statement_timeout(10 * timeout_sec, verbose=False)
            print(f"Creating index on {t}.{c}")
            index_creation_start = time.perf_counter()
            try:
                index_name = db_conn.create_index(t, c)
                existing_indexes[(t, c)] = index_name
                print(
                    f"Creation time: {time.perf_counter() - index_creation_start:.2f}s"
                )
            except Exception as e:
                print(f"Index creation failed {str(e)}")
            db_conn.set_statement_timeout(timeout_sec, verbose=False)

        # indexes for all columns, delete one
        if len(cols) > 1 and no_idxs == len(cols):
            t, c = random.choice(cols)
            print(f"Dropping index on {t}.{c}")
            try:
                index_name = existing_indexes[(t, c)]
                db_conn.drop_index(index_name)
                del existing_indexes[(t, c)]
            except Exception as e:
                print(f"Index deletion failed {str(e)}")


def modify_indexes(db_conn, sql_query, existing_indexes, timeout_sec):
    try:
        if "GROUP BY " in sql_query:
            sql_query = sql_query.split("GROUP BY ")[0]
        join_part = sql_query.split(" FROM ")[1].split(" WHERE ")[0]
        where_part = sql_query.split(" FROM ")[1].split(" WHERE ")[1]

        index_creation_deletion(existing_indexes, join_part, db_conn, timeout_sec)
        index_creation_deletion(existing_indexes, where_part, db_conn, timeout_sec)
    except Exception as e:
        print(f"Could not create indexes for {sql_query} ({str(e)})")


def run_aurora_workload(
    workload_path,
    database,
    db_name,
    database_conn_args,
    database_kwarg_dict,
    target_path,
    run_kwargs,
    repetitions_per_query,
    timeout_sec,
    cap_workload,
    rank,
    world_size,
):
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    db_conn = create_db_conn(database, db_name, database_conn_args, database_kwarg_dict)

    with open(workload_path) as f:
        content = f.readlines()
    sql_queries = [x.strip() for x in content]

    start_offset, end_offset = compute_workload_splits(
        len(sql_queries), rank, world_size
    )
    print("----------------------------------")
    print("Rank:", rank)
    print("World size:", world_size)
    print("Running queries in range: [{}, {})".format(start_offset, end_offset))
    print("----------------------------------")
    relevant_queries = sql_queries[start_offset:end_offset]

    hint_list = ["" for _ in sql_queries]

    # extract column statistics
    database_stats = db_conn.collect_db_statistics()

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

    # set a timeout to make sure long running queries do not delay the entire process
    db_conn.set_statement_timeout(timeout_sec)

    existing_indexes = dict()

    # extract query plans
    start_t = time.perf_counter()
    valid_queries = 0
    for i, sql_query in enumerate(tqdm(relevant_queries)):
        if cap_workload and i >= cap_workload:
            break
        if sql_query in seen_queries:
            continue

        hint = hint_list[i]
        curr_statistics = db_conn.run_query_collect_statistics(
            sql_query, repetitions=repetitions_per_query, prefix=hint
        )
        curr_statistics.update(sql=sql_query)
        curr_statistics.update(hint=hint)
        curr_statistics.update(query_index=i)
        query_list.append(curr_statistics)

        run_stats = dict(
            query_list=query_list,
            database_stats=database_stats,
            run_kwargs=run_kwargs,
            total_time_secs=time_offset + (time.perf_counter() - start_t),
        )

        # save to json
        # write to temporary path and then move
        if len(query_list) % 50 == 0:
            save_workload(run_stats, target_path)

    run_stats = dict(
        query_list=query_list,
        database_stats=database_stats,
        run_kwargs=run_kwargs,
        total_time_secs=time_offset + (time.perf_counter() - start_t),
    )
    save_workload(run_stats, target_path)

    print(
        f"Executed workload {workload_path} in {time_offset + time.perf_counter() - start_t:.2f} secs"
    )


def re_execute_query_with_no_result(
    workload_path, database_conn_args, database_kwarg_dict
):
    # Due to connection error, sometimes we get queries with no results
    n_query = 0
    aurora_raw = load_json(workload_path)
    db_conn = create_db_conn(
        DatabaseSystem.AURORA, "imdb", database_conn_args, database_kwarg_dict
    )
    for i, q in enumerate(aurora_raw.query_list):
        if q.verbose_plan is None:
            print(f"executing query {i}")
            curr_statistics = db_conn.run_query_collect_statistics(
                q.sql, repetitions=1, prefix=""
            )
            curr_statistics.update(sql=q.sql)
            curr_statistics.update(hint="")
            aurora_raw.query_list[i] = curr_statistics
            n_query += 1
            if n_query % 20 == 0:
                save_workload(aurora_raw, workload_path)
    save_workload(aurora_raw, workload_path)


def save_workload(run_stats, target_path):
    target_temp_path = os.path.join(
        os.path.dirname(target_path), f"{os.path.basename(target_path)}_temp"
    )
    with open(target_temp_path, "w") as outfile:
        json.dump(run_stats, outfile, default=str)
    shutil.move(target_temp_path, target_path)
