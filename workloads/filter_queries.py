import pathlib
import sys
import argparse
import json
import collections
from tqdm import tqdm

sys.path.append(str(pathlib.Path(__file__).parents[1]))  # HACK
import workloads.cross_db_benchmark.benchmark_tools.aurora.parse_plan as pp


def main():
    # This script removes queries that have zero cardinality and/or timed out.
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-run", type=str, required=True)
    parser.add_argument("--out-file", type=str, required=True)
    args = parser.parse_args()

    with open(args.raw_run) as file:
        raw_data = json.load(file)

    # keep track of column statistics
    column_id_mapping = dict()
    table_id_mapping = dict()
    partial_column_name_mapping = collections.defaultdict(set)

    database_stats = raw_data["database_stats"]
    # enrich column stats with table sizes
    table_sizes = dict()
    for table_stat in database_stats["table_stats"]:
        table_sizes[table_stat["relname"]] = table_stat["reltuples"]

    for i, column_stat in enumerate(database_stats["column_stats"]):
        table = column_stat["tablename"]
        column = column_stat["attname"]
        column_stat.table_size = table_sizes[table]
        column_id_mapping[(table, column)] = i
        partial_column_name_mapping[column].add(table)

    # similar for table statistics
    for i, table_stat in enumerate(database_stats["table_stats"]):
        table = table_stat["relname"]
        table_id_mapping[table] = i

    timeout_count = 0
    num_zero_card = 0
    num_too_short = 0
    num_too_long = 0
    good_queries = []

    for query_no, q in tqdm(enumerate(raw_data["query_list"])):
        if q["timeout"]:
            timeout_count += 1
            continue

        if "analyze_plans" not in q or q["analyze_plans"] is None:
            print(f"parsed_query {query_no} no analyze plans")
            continue

        if len(q["analyze_plans"]) == 0:
            print(f"parsed_query {query_no} no analyze plans")
            continue

        # subqueries are currently not supported
        analyze_str = "".join([l[0] for l in q["verbose_plan"]])
        if "SubPlan" in analyze_str or "InitPlan" in analyze_str:
            print(f"parsed_query {query_no} contains SubPlan or InitPlan")
            continue

        # subquery is empty due to logical constraints
        if "->  Result  (cost=0.00..0.00 rows=0" in analyze_str:
            print(f"parsed_query {query_no} is empty")
            continue

        # compute average execution and planning times
        ex_times = []
        for analyze_plan in q["analyze_plans"]:
            _, ex_time, _ = pp.parse_plan(analyze_plan, analyze=True, parse=False)
            ex_times.append(ex_time)
        avg_runtime = sum(ex_times) / len(ex_times)

        # parse the plan as a tree
        analyze_plan, _, _ = pp.parse_plan(
            q["analyze_plans"][0], analyze=True, parse=True
        )

        # parse information contained in operator nodes (different information in verbose and analyze plan)
        alias_dict = dict()
        analyze_plan.parse_lines_recursively(
            alias_dict=alias_dict,
            parse_baseline=False,
            parse_join_conds=False,
            is_brad=True,
        )

        # only explain plan (not executed)
        verbose_plan, _, _ = pp.parse_plan(q["verbose_plan"], analyze=False, parse=True)
        verbose_plan.parse_lines_recursively(
            alias_dict=alias_dict,
            parse_baseline=False,
            parse_join_conds=False,
            is_brad=True,
        )

        # merge the plans with different information
        analyze_plan.merge_recursively(verbose_plan)

        analyze_plan.parse_columns_bottom_up(
            column_id_mapping,
            partial_column_name_mapping,
            table_id_mapping,
            alias_dict=alias_dict,
        )

        min_runtime = 100
        max_runtime = 200000

        # check if result is None
        if analyze_plan.min_card() == 0:
            print(f"parsed_query {query_no} has zero_card")
            num_zero_card += 1
            continue

        elif analyze_plan.min_card() == 0 and avg_runtime < min_runtime:
            print(f"parsed_query {query_no} has zero_card and runtime too small")
            num_too_short += 1
            continue

        if min_runtime is not None and avg_runtime < min_runtime:
            print(f"parsed_query {query_no} runtime too small")
            num_too_short += 1
            continue

        if avg_runtime > max_runtime:
            print(f"parsed_query {query_no} runtime too large")
            num_too_long += 1
            continue

        good_queries.append(q)

    print("Timeouts:", timeout_count)
    print("Zero card:", num_zero_card)
    print("Too short:", num_too_short)
    print("Too long:", num_too_long)
    print("OK:", len(good_queries))

    with open(args.out_file, "w") as file:
        json.dump(good_queries, file)


if __name__ == "__main__":
    main()
