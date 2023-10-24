import json
from typing import Optional
from workloads.cross_db_benchmark.benchmark_tools.utils import load_json, dumper


DATA_AUG_DIST = {10: 1, 20: 4, 50: 10, 100: 20, 500: 40}

# AUG_DIST_AURORA_100 = {2: 1, 20: 2, 50: 5, 100: 15, 500: 30}
# AUG_DIST_REDSHIFT_100 = {10: 1, 20: 8, 50: 20, 100: 40, 500: 80}

AUG_DIST_AURORA_100 = {2: 1, 20: 2, 50: 5, 100: 12, 500: 30}
AUG_DIST_REDSHIFT_100 = {3: 1, 10: 2, 20: 8, 50: 20, 100: 40, 500: 80}

_custom_dists = {
    "redshift_100g": AUG_DIST_REDSHIFT_100,
    "aurora_100g": AUG_DIST_AURORA_100,
}


def augment_dataset(source, target, custom_dist_name: Optional[str] = None):
    # Some dataset contains very few long-running queries. For the purpose of identifying bad queries, we duplicate
    # long-running queries.
    runs = load_json(source, namespace=False)
    new_parsed_queries = []
    new_sql_queries = []
    new_parsed_plans = []

    if custom_dist_name is not None:
        dist = _custom_dists[custom_dist_name]
        print(f"Using {custom_dist_name} distribution.")
    else:
        dist = DATA_AUG_DIST
        print("Using default distribution.")

    for i, q in enumerate(runs["parsed_queries"]):
        sql = runs["sql_queries"][i]
        runtime = q["plan_runtime"] / 1000  # ms to s convertion
        plan = runs["parsed_plans"][i]
        matched = False

        for upper_limit, dup_times in dist.items():
            if runtime <= upper_limit:
                duplicated_query = [q] * dup_times
                duplicated_sql = [sql] * dup_times
                duplicated_plan = [plan] * dup_times
                new_parsed_queries.extend(duplicated_query)
                new_sql_queries.extend(duplicated_sql)
                new_parsed_plans.extend(duplicated_plan)
                matched = True
                break
        if not matched:
            new_parsed_queries.append(q)
            new_sql_queries.append(sql)

    argmented_runs = {
        "database_stats": runs["database_stats"],
        "run_kwargs": runs["run_kwargs"],
        "parsed_queries": new_parsed_queries,
        "sql_queries": new_sql_queries,
        "parsed_plans": new_parsed_plans,
    }
    with open(target, "w") as outfile:
        json.dump(argmented_runs, outfile, default=dumper)
