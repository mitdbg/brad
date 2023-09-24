import collections
import json
import os.path
import numpy as np
import statistics
import psycopg2
from tqdm import tqdm

from workloads.cross_db_benchmark.benchmark_tools.database import DatabaseSystem
from workloads.cross_db_benchmark.benchmark_tools.aurora.utils import plan_statistics
from workloads.cross_db_benchmark.benchmark_tools.aurora.parse_plan import parse_plan
from workloads.cross_db_benchmark.benchmark_tools.load_database import create_db_conn
from workloads.cross_db_benchmark.benchmark_tools.utils import load_json, dumper
from workloads.cross_db_benchmark.benchmark_tools.aurora.parse_query import (
    refine_db_stats,
    parse_query_from_plan,
)


def parse_queries_redshift(
    run_stats,
    aurora_run_stats,
    min_runtime=100,
    max_runtime=300000,
    use_true_card=True,
    include_timeout=False,
    db_name=None,
    database_conn_args=None,
    database_kwarg_dict=None,
    LOCAL_DSN="host=/tmp ",
    timeout_ms=None,
    query_cache_file=None,
    save_cache=False,
    cap_queries=None,
    target_path=None,
    is_brad=False,
):
    assert len(run_stats.query_list) == len(aurora_run_stats.query_list)
    db_conn = None
    cursor = None
    if database_conn_args is not None:
        db_conn = create_db_conn(
            DatabaseSystem.AURORA, db_name, database_conn_args, database_kwarg_dict
        )
        cursor = None
    elif LOCAL_DSN is not None:
        LOCAL_DSN += f"dbname={db_name}"
        conn = psycopg2.connect(LOCAL_DSN)
        cursor = conn.cursor()
        db_conn = None

    if query_cache_file:
        cache = load_json(query_cache_file, namespace=False)
    else:
        cache = dict()
    column_id_mapping = dict()
    table_id_mapping = dict()

    partial_column_name_mapping = collections.defaultdict(set)

    database_stats = refine_db_stats(aurora_run_stats.database_stats, db_conn, cursor)
    # parse individual queries
    parsed_plans = []
    parsed_queries = []
    sql_queries = []
    avg_runtimes = []
    no_tables = []
    no_filters = []
    start_idx = 0
    if target_path is not None and os.path.exists(target_path):
        # TODO: loading from previous parsed plan is somewhat incorrect
        try:
            parsed_runs = load_json(target_path)
            parsed_plans = parsed_runs.parsed_plans
            parsed_queries = parsed_runs.parsed_queries
            sql_queries = parsed_runs.sql_queries
            assert len(parsed_plans) == len(parsed_queries) == len(sql_queries)
            database_stats = parsed_runs.database_stats
            start_idx = len(parsed_plans)
        except:
            start_idx = 0

    if hasattr(run_stats, "run_kwargs"):
        run_kwargs = run_stats.run_kwargs
    else:
        run_kwargs = None

    # enrich column stats with table sizes
    table_sizes = dict()
    for table_stat in database_stats.table_stats:
        table_sizes[table_stat.relname] = table_stat.reltuples

    for i, column_stat in enumerate(database_stats.column_stats):
        table = column_stat.tablename
        column = column_stat.attname
        column_stat.table_size = table_sizes[table]
        column_id_mapping[(table, column)] = i
        partial_column_name_mapping[column].add(table)

    # similar for table statistics
    for i, table_stat in enumerate(database_stats.table_stats):
        table = table_stat.relname
        table_id_mapping[table] = i

    for query_no, q in enumerate(tqdm(run_stats.query_list[start_idx:])):
        is_timeout = False
        query_no += start_idx
        aurora_q = aurora_run_stats.query_list[query_no]
        assert q.sql == aurora_q.sql or q.sql == aurora_q.sql + "\n", query_no
        if hasattr(q, "error") and q.error:
            continue
        # do not parse timeout queries
        if hasattr(q, "timeout") and q.timeout:
            if include_timeout:
                is_timeout = True
            else:
                continue

        alias_dict = dict()
        if is_timeout:
            runtime = max_runtime * np.uniform(1, 3)
        elif hasattr(q, "runtimes") and len(q.runtimes) > 1:
            # We ran for more than one repetition.
            # Always discard the first run to exclude compilation overhead (Redshift).
            # Note that this function also runs to parse Athena results. But we
            # only run one repetition on Athena, so this branch should not run.
            runtime = statistics.mean(q.runtimes[1:]) * 1000
        else:
            runtime = q.runtime * 1000

        # only explain plan (not executed)
        if aurora_q.verbose_plan is None:
            print(f"Aurora has no verbose plan for query {query_no}")
            continue
        verbose_plan, _, _ = parse_plan(
            aurora_q.verbose_plan, analyze=False, parse=True
        )
        verbose_plan.parse_lines_recursively(
            alias_dict=alias_dict,
            parse_baseline=False,
            parse_join_conds=False,
            is_brad=is_brad,
        )

        tables, filter_columns, operators = plan_statistics(verbose_plan)

        verbose_plan.parse_columns_bottom_up(
            column_id_mapping,
            partial_column_name_mapping,
            table_id_mapping,
            alias_dict=alias_dict,
        )
        verbose_plan.tables = tables
        verbose_plan.num_tables = len(tables)
        verbose_plan.plan_runtime = runtime

        def augment_no_workers(p, top_no_workers=0):
            no_workers = p.plan_parameters.get("workers_planned")
            if no_workers is None:
                no_workers = top_no_workers

            p.plan_parameters["workers_planned"] = top_no_workers

            for c in p.children:
                augment_no_workers(c, top_no_workers=no_workers)

        augment_no_workers(verbose_plan)

        if min_runtime is not None and runtime < min_runtime:
            print(f"parsed_query {query_no} runtime too small")
            continue

        if runtime > max_runtime:
            print(f"parsed_query {query_no} runtime too large")
            continue

        # parsed_query = None
        parsed_query = parse_query_from_plan(
            alias_dict,
            database_stats,
            verbose_plan,
            q.sql,
            column_id_mapping,
            table_id_mapping,
            is_explain_only=True,
            use_true_card=use_true_card,
            db_conn=db_conn,
            cursor=cursor,
            timeout_ms=timeout_ms,
            return_namespace=False,
            is_brad=is_brad,
            cache=cache,
        )
        if "tables" in verbose_plan:
            verbose_plan["tables"] = list(verbose_plan["tables"])
        else:
            verbose_plan["tables"] = []
        if parsed_query is not None and len(parsed_query["join_nodes"]) != 0:
            parsed_queries.append(parsed_query)
            parsed_plans.append(verbose_plan)
            sql_queries.append(q.sql)
        elif parsed_query is None:
            print(f"parsed_query {query_no} is none")
        else:
            print(f"parsed_query {query_no} has no join")

        if cap_queries is not None and len(parsed_plans) >= cap_queries:
            print(f"Parsed {cap_queries} queries. Stopping parsing.")
            break

        if target_path is not None and len(parsed_plans) % 100 == 0:
            parsed_runs = dict(
                parsed_plans=parsed_plans,
                parsed_queries=parsed_queries,
                sql_queries=sql_queries,
                database_stats=database_stats,
                run_kwargs=run_kwargs,
            )
            with open(target_path, "w") as outfile:
                json.dump(parsed_runs, outfile, default=dumper)

    parsed_runs = dict(
        parsed_plans=parsed_plans,
        parsed_queries=parsed_queries,
        sql_queries=sql_queries,
        database_stats=database_stats,
        run_kwargs=run_kwargs,
    )

    stats = dict(
        runtimes=str(avg_runtimes), no_tables=str(no_tables), no_filters=str(no_filters)
    )

    if query_cache_file and save_cache:
        with open(query_cache_file, "w") as outfile:
            json.dump(run_stats, outfile)

    return parsed_runs, stats
