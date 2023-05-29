import json
from workloads.cross_db_benchmark.benchmark_tools.utils import load_json, dumper


DATA_ARG_DIST = {10: 1, 20: 4, 50: 20, 200: 40}


def argment_dataset(source, target):
    # Some dataset contains very few long-running queries. For the purpose of identifying bad queries, we duplicate
    # long-running queries.
    runs = load_json(source, namespace=False)
    new_parsed_queries = []
    new_sql_queries = []
    for i, q in enumerate(runs["parsed_queries"]):
        sql = runs["sql_queries"][i]
        runtime = q["plan_runtime"] / 1000  # ms to s convertion
        matched = False
        for upper_limit in DATA_ARG_DIST:
            if runtime <= upper_limit:
                duplicated_query = [q] * DATA_ARG_DIST[upper_limit]
                duplicated_sql = [sql] * DATA_ARG_DIST[upper_limit]
                new_parsed_queries.extend(duplicated_query)
                new_sql_queries.extend(duplicated_sql)
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
    }
    with open(target, "w") as outfile:
        json.dump(argmented_runs, outfile, default=dumper)
