import json
import os

from workloads.cross_db_benchmark.benchmark_tools.utils import load_json, dumper
from workloads.cross_db_benchmark.benchmark_tools.database import DatabaseSystem
from workloads.cross_db_benchmark.benchmark_tools.postgres.combine_plans import (
    combine_traces,
)
from workloads.cross_db_benchmark.benchmark_tools.postgres.parse_plan import (
    parse_plans_postgres,
)
from workloads.cross_db_benchmark.benchmark_tools.postgres.parse_query import (
    parse_plans_with_query_postgres,
)
from workloads.cross_db_benchmark.benchmark_tools.aurora.parse_plan import (
    parse_plans_aurora,
)
from workloads.cross_db_benchmark.benchmark_tools.aurora.parse_query import (
    parse_plans_with_query_aurora,
)
from workloads.cross_db_benchmark.benchmark_tools.redshift.parse_query import (
    parse_queries_redshift,
)
from workloads.cross_db_benchmark.benchmark_tools.athena.parse_query import (
    parse_queries_athena,
)
from workloads.cross_db_benchmark.benchmark_tools.aurora.utils import plan_statistics
from workloads.cross_db_benchmark.benchmark_tools.aurora.parse_plan import parse_plan
from workloads.cross_db_benchmark.benchmark_tools.aurora.parse_query import (
    parse_query_from_plan,
)


def parse_run(
    source_paths,
    target_path,
    database,
    min_query_ms=100,
    max_query_ms=30000,
    parse_baseline=False,
    cap_queries=None,
    parse_join_conds=False,
    include_zero_card=False,
    explain_only=False,
):
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    if database == DatabaseSystem.POSTGRES:
        parse_func = parse_plans_postgres
        comb_func = combine_traces
    elif database == DatabaseSystem.AURORA:
        parse_func = parse_plans_aurora
        comb_func = combine_traces
    else:
        raise NotImplementedError(f"Database {database} not yet supported.")

    if not isinstance(source_paths, list):
        source_paths = [source_paths]

    assert all([os.path.exists(p) for p in source_paths])
    run_stats = [load_json(p) for p in source_paths]
    run_stats = comb_func(run_stats)

    parsed_runs, stats = parse_func(
        run_stats,
        min_runtime=min_query_ms,
        max_runtime=max_query_ms,
        parse_baseline=parse_baseline,
        cap_queries=cap_queries,
        parse_join_conds=parse_join_conds,
        include_zero_card=include_zero_card,
        explain_only=explain_only,
    )

    with open(target_path, "w") as outfile:
        json.dump(parsed_runs, outfile, default=dumper)
    return len(parsed_runs["parsed_plans"]), stats


def parse_plans(
    database,
    run_stats,
    min_runtime=100,
    max_runtime=100000,
    parse_baseline=False,
    cap_queries=None,
    parse_join_conds=False,
    include_zero_card=False,
    explain_only=False,
):
    if database == DatabaseSystem.POSTGRES:
        return parse_plans_postgres(
            run_stats,
            min_runtime,
            max_runtime,
            parse_baseline,
            cap_queries,
            parse_join_conds,
            include_zero_card,
            explain_only,
        )
    elif database == DatabaseSystem.AURORA:
        return parse_plans_aurora(
            run_stats,
            min_runtime,
            max_runtime,
            parse_baseline,
            cap_queries,
            parse_join_conds,
            include_zero_card,
            explain_only,
        )
    else:
        raise NotImplementedError(f"Database {database} not yet supported.")


def parse_queries(
    database,
    run_stats,
    run_stats_aurora=None,
    min_runtime=100,
    max_runtime=3000000,
    include_zero_card=False,
    include_timeout=False,
    explain_only=False,
    use_true_card=True,
    db_name=None,
    database_conn_args=None,
    database_kwarg_dict=None,
    LOCAL_DSN="host=/tmp ",
    timeout_ms=None,
    query_cache_file=None,
    zero_card_min_runtime=None,
    save_cache=False,
    cap_queries=None,
    target_path=None,
    is_brad=False,
    include_no_joins=False,
):
    if database == DatabaseSystem.POSTGRES:
        return parse_plans_with_query_postgres(
            run_stats,
            min_runtime=min_runtime,
            max_runtime=max_runtime,
            include_zero_card=include_zero_card,
            explain_only=explain_only,
            use_true_card=use_true_card,
            db_name=db_name,
            database_conn_args=database_conn_args,
            database_kwarg_dict=database_kwarg_dict,
            LOCAL_DSN=LOCAL_DSN,
            timeout_ms=timeout_ms,
            query_cache_file=query_cache_file,
            zero_card_min_runtime=zero_card_min_runtime,
            save_cache=save_cache,
            cap_queries=cap_queries,
            target_path=target_path,
        )
    elif database == DatabaseSystem.AURORA:
        return parse_plans_with_query_aurora(
            run_stats,
            min_runtime=min_runtime,
            max_runtime=max_runtime,
            include_zero_card=include_zero_card,
            include_timeout=include_timeout,
            explain_only=explain_only,
            use_true_card=use_true_card,
            db_name=db_name,
            database_conn_args=database_conn_args,
            database_kwarg_dict=database_kwarg_dict,
            LOCAL_DSN=LOCAL_DSN,
            timeout_ms=timeout_ms,
            query_cache_file=query_cache_file,
            zero_card_min_runtime=zero_card_min_runtime,
            save_cache=save_cache,
            cap_queries=cap_queries,
            target_path=target_path,
            is_brad=is_brad,
            include_no_joins=include_no_joins,
        )
    elif database == DatabaseSystem.REDSHIFT:
        return parse_queries_redshift(
            run_stats,
            run_stats_aurora,
            min_runtime=min_runtime,
            max_runtime=max_runtime,
            use_true_card=use_true_card,
            include_timeout=include_timeout,
            db_name=db_name,
            database_conn_args=database_conn_args,
            database_kwarg_dict=database_kwarg_dict,
            LOCAL_DSN=LOCAL_DSN,
            timeout_ms=timeout_ms,
            query_cache_file=query_cache_file,
            save_cache=save_cache,
            cap_queries=cap_queries,
            target_path=target_path,
            is_brad=is_brad,
            include_no_joins=include_no_joins,
        )
    elif database == DatabaseSystem.ATHENA:
        return parse_queries_athena(
            run_stats,
            run_stats_aurora,
            min_runtime=min_runtime,
            max_runtime=max_runtime,
            use_true_card=use_true_card,
            include_timeout=include_timeout,
            db_name=db_name,
            database_conn_args=database_conn_args,
            database_kwarg_dict=database_kwarg_dict,
            LOCAL_DSN=LOCAL_DSN,
            timeout_ms=timeout_ms,
            query_cache_file=query_cache_file,
            save_cache=save_cache,
            cap_queries=cap_queries,
            target_path=target_path,
            is_brad=is_brad,
            include_no_joins=include_no_joins,
        )
    else:
        raise NotImplementedError(f"Database {database} not yet supported.")


def parse_query_generic(
    verbose_plan,
    sql,
    db_conn,
    database_stats,
    column_id_mapping,
    table_id_mapping,
    partial_column_name_mapping,
):
    # parse query from Aurora's EXPLAIN which can be used by other engines
    alias_dict = dict()
    verbose_plan, _, _ = parse_plan(verbose_plan, analyze=False, parse=True)
    verbose_plan.parse_lines_recursively(
        alias_dict=alias_dict,
        parse_baseline=False,
        parse_join_conds=False,
        is_brad=True,
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

    def augment_no_workers(p, top_no_workers=0):
        no_workers = p.plan_parameters.get("workers_planned")
        if no_workers is None:
            no_workers = top_no_workers

        p.plan_parameters["workers_planned"] = top_no_workers

        for c in p.children:
            augment_no_workers(c, top_no_workers=no_workers)

    augment_no_workers(verbose_plan)
    parsed_query = parse_query_from_plan(
        alias_dict,
        database_stats,
        verbose_plan,
        sql,
        column_id_mapping,
        table_id_mapping,
        is_explain_only=True,
        use_true_card=False,
        db_conn=db_conn,
        return_namespace=True,
        is_brad=True,
    )
    if parsed_query is not None and len(parsed_query.join_nodes) != 0:
        # TODO: need to test the case of no join, if the model works fine
        return parsed_query
    else:
        return None
