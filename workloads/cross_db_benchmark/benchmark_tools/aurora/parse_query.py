import collections
import json
import numpy as np
import pandas as pd
import psycopg2
from tqdm import tqdm
from types import SimpleNamespace
from typing import Dict, Any

from workloads.cross_db_benchmark.benchmark_tools.database import DatabaseSystem
from workloads.cross_db_benchmark.benchmark_tools.aurora.utils import (
    plan_statistics,
    getJoinConds,
    getFilters,
)
from workloads.cross_db_benchmark.benchmark_tools.aurora.parse_plan import (
    parse_plan,
    init_plan_regex,
)
from workloads.cross_db_benchmark.benchmark_tools.load_database import create_db_conn
from workloads.cross_db_benchmark.benchmark_tools.aurora.aurora_executor import (
    get_true_card,
    get_est_card,
)
from workloads.cross_db_benchmark.benchmark_tools.utils import load_json, dumper


def dict_to_namespace(d):
    namespace = SimpleNamespace()
    for key, value in d.items():
        if isinstance(key, int) or isinstance(key, float):
            key = str(key)
        if isinstance(value, dict):
            setattr(namespace, key, dict_to_namespace(value))
        elif isinstance(value, list):
            setattr(
                namespace,
                key,
                [
                    dict_to_namespace(item) if isinstance(item, dict) else item
                    for item in value
                ],
            )
        else:
            setattr(namespace, key, value)
    return namespace


def create_scan_node(
    filter_node,
    filter_text,
    database_stats,
    use_true_card,
    db_conn=None,
    cursor=None,
    timeout_ms=None,
    cache=None,
    table_alias=None,
):
    new_filter_node = dict()
    table = filter_node["table"]
    table_size = database_stats.table_stats[table].reltuples
    new_filter_node["table"] = table
    new_filter_node["plan_parameters"] = {
        "op_name": "scan",
        "act_card": 1,
        "est_card": filter_node["est_card"],
        "act_children_card": table_size,
        "est_children_card": table_size,
        "est_width": filter_node["est_width"],
    }
    has_timeout = False
    if filter_text is None:
        new_filter_node["filter_columns"] = None
        new_filter_node["plan_parameters"]["est_card"] = table_size
        if use_true_card:
            new_filter_node["plan_parameters"]["act_card"] = table_size
    else:
        new_filter_node["filter_columns"] = filter_node["filter_columns"]
        if use_true_card:
            if "act_card" in filter_node and filter_node["act_card"]:
                new_filter_node["plan_parameters"]["act_card"] = filter_node["act_card"]
            else:
                table_name = database_stats.table_stats[table].relname
                if table_alias:
                    sql = f'SELECT COUNT(*) FROM "{table_name}" AS "{table_alias}" WHERE {filter_text};'
                else:
                    sql = f'SELECT COUNT(*) FROM "{table_name}" WHERE {filter_text};'
                if cache is not None and sql in cache:
                    true_card = cache[sql]
                elif cursor is not None:
                    true_card, curr_has_timeout = get_true_card(sql, cursor, timeout_ms)
                    if curr_has_timeout:
                        has_timeout = True
                else:
                    assert db_conn is not None, "no valid database connection"
                    true_card = db_conn.get_result(sql)[0][0]
                if cache is not None:
                    cache[sql] = true_card
                new_filter_node["plan_parameters"]["act_card"] = true_card
        else:
            new_filter_node["plan_parameters"]["act_card"] = filter_node["est_card"]

    if "output_columns" in filter_node:
        new_filter_node["output_columns"] = filter_node["output_columns"]
    else:
        new_filter_node["output_columns"] = []
    return new_filter_node, has_timeout


def create_join_node(
    join_cond,
    joined_tables,
    join_table_alias,
    scan_nodes,
    filters,
    database_stats,
    use_true_card,
    db_conn=None,
    cursor=None,
    timeout_ms=None,
    cache=None,
    alias_dict=None,
):
    (t1, k1, t2, k2) = joined_tables
    join_node = dict()
    join_node["plan_parameters"] = {
        "op_name": "join",
        "act_card": 1,
        "est_card": 1,
        "act_children_card": 1,
        "est_children_card": 1,
        "est_width": 1,
    }
    join_node["filter_columns"] = {
        "columns": [k1, k2],
        "operator": "=",
        "literal_feature": 0,
    }
    join_node["tables"] = [t1, t2]
    join_node["table_alias"] = join_table_alias
    t1_name = database_stats.table_stats[t1].relname
    t2_name = database_stats.table_stats[t2].relname

    if join_table_alias == [None, None]:
        # no table alias the scan node is ided by table itself
        if t1 not in scan_nodes or t2 not in scan_nodes:
            # print(f"WARNING Aurora omitted one of your join condition {join_cond}")
            return None, None
        scan_node_t1 = scan_nodes[t1]
        scan_node_t2 = scan_nodes[t2]
        filter_t1 = filters[t1]
        filter_t2 = filters[t2]
        t1_name = f'"{t1_name}"'
        t2_name = f'"{t2_name}"'
    else:
        at1 = join_table_alias[0]
        at2 = join_table_alias[1]
        if at1 not in scan_nodes or at2 not in scan_nodes:
            # print(f"WARNING Aurora omitted one of your join condition {join_cond}")
            return None, None
        scan_node_t1 = scan_nodes[at1]
        scan_node_t2 = scan_nodes[at2]
        filter_t1 = filters[at1]
        filter_t2 = filters[at2]
        t1_name = f'"{t1_name}" AS "{at1}"'
        t2_name = f'"{t2_name}" AS "{at2}"'

    join_node["plan_parameters"]["act_children_card"] = (
        scan_node_t1["plan_parameters"]["act_card"]
        * scan_node_t2["plan_parameters"]["act_card"]
    )
    join_node["plan_parameters"]["est_children_card"] = (
        scan_node_t1["plan_parameters"]["est_card"]
        * scan_node_t2["plan_parameters"]["est_card"]
    )
    filter_cond = ""
    has_timeout = False
    if filter_t1:
        filter_cond += " AND " + filter_t1
    if filter_t2:
        filter_cond += " AND " + filter_t2

    sql = f"SELECT COUNT(*) FROM {t1_name}, {t2_name} WHERE {join_cond}{filter_cond};"
    if cursor:
        est_card, est_width = get_est_card(sql, cursor, return_width=True)
    elif db_conn:
        _, temp_cursor = db_conn.get_cursor()
        est_card, est_width = get_est_card(sql, temp_cursor, return_width=True)
    else:
        est_card = join_node["plan_parameters"]["children_card"]
        est_width = (
            scan_node_t1["plan_parameters"]["est_width"]
            + scan_node_t2["plan_parameters"]["est_width"]
        )
    join_node["plan_parameters"]["est_card"] = est_card
    join_node["plan_parameters"]["est_width"] = est_width
    if est_card is None:
        return join_node, True

    if use_true_card:
        if cache is not None and sql in cache:
            true_card = cache[sql]
        elif cursor is not None:
            true_card, curr_has_timeout = get_true_card(sql, cursor, timeout_ms)
            if curr_has_timeout:
                has_timeout = True
        else:
            assert db_conn is not None, "no valid database connection"
            true_card = db_conn.get_result(sql)[0][0]
        if cache is not None:
            cache[sql] = true_card
        join_node["plan_parameters"]["act_card"] = true_card
    else:
        join_node["plan_parameters"]["act_card"] = est_card
        join_node["plan_parameters"]["act_children_card"] = join_node[
            "plan_parameters"
        ]["est_children_card"]
    return join_node, has_timeout


def parse_query_from_plan(
    alias_dict,
    database_stats,
    raw_plan,
    sql,
    column_id_mapping,
    table_id_mapping,
    is_explain_only=True,
    use_true_card=False,
    db_conn=None,
    cursor=None,
    timeout_ms=None,
    cache=None,
    timeout_ok=False,
    return_namespace=False,
    is_brad=False,
):
    join_conds = getJoinConds(
        alias_dict, sql, table_id_mapping, column_id_mapping, is_brad
    )
    if len(join_conds) == 0:
        return None
    filter_nodes, filter_texts = getFilters(raw_plan, True)
    parsed_query = dict()
    parsed_query["plan_parameters"] = {
        "op_name": "embedding",
        "num_tables": len(filter_nodes),
        "num_joins": len(join_conds),
    }
    parsed_query["output_columns"] = raw_plan["plan_parameters"]["output_columns"]
    parsed_query["num_tables"] = len(filter_nodes)
    parsed_query["aurora_omit_join_cond"] = []
    has_timeout = False
    if is_explain_only:
        parsed_query["plan_runtime"] = 0
    else:
        parsed_query["plan_runtime"] = raw_plan["plan_runtime"]
    parsed_query["join_nodes"] = []

    scan_nodes = dict()
    for table in filter_nodes:
        if alias_dict is not None and len(alias_dict) != 0:
            table_alias = table
        else:
            table_alias = None
        scan_node, has_timeout = create_scan_node(
            filter_nodes[table],
            filter_texts[table],
            database_stats,
            use_true_card,
            db_conn,
            cursor,
            timeout_ms,
            cache,
            table_alias=table_alias,
        )
        if scan_node is None or (has_timeout and not timeout_ok):
            return None
        scan_nodes[table] = scan_node
    parsed_query["scan_nodes"] = scan_nodes

    for join_cond in join_conds:
        joined_tables, join_table_alias = join_conds[join_cond]
        join_node, has_timeout = create_join_node(
            join_cond,
            joined_tables,
            join_table_alias,
            scan_nodes,
            filter_texts,
            database_stats,
            use_true_card,
            db_conn,
            cursor,
            timeout_ms,
            cache,
            alias_dict,
        )
        if has_timeout and not timeout_ok:
            return None
        if join_node is not None:
            parsed_query["join_nodes"].append(join_node)
        else:
            parsed_query["aurora_omit_join_cond"].append(join_cond)
    if return_namespace and parsed_query is not None:
        parsed_query = dict_to_namespace(parsed_query)
    return parsed_query


def refine_db_stats(database_stats, db_conn=None, cursor=None):
    table_stats = database_stats.table_stats
    column_stats = database_stats.column_stats
    if "has_index" in column_stats[0].__dict__ and "relcols" in table_stats[0].__dict__:
        return database_stats

    index_sql = "SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' ORDER BY tablename, indexname;"
    if cursor:
        cursor.execute(index_sql)
        indexes = cursor.fetchall()
    elif db_conn:
        _, cursor = db_conn.get_cursor(db_created=True)
        cursor.execute(index_sql)
        indexes = cursor.fetchall()
    else:
        return database_stats

    # We currently assume only PK and FK has index, and table is sorted based on PK
    # will be very easy to extend to other situations with an existing schema.
    all_index_by_column = dict()
    for index in indexes:
        table_name = index[0]
        column_name = index[-1].split("(")[-1].split(")")[0]
        is_pk = " UNIQUE " in index[-1]
        if is_pk:
            all_index_by_column[(table_name, column_name)] = (1, 0, 1)
        else:
            all_index_by_column[(table_name, column_name)] = (0, 1, 0)

    num_cols = dict()
    for col in column_stats:
        table_name = col.tablename
        column_name = col.attname
        if table_name not in num_cols:
            num_cols[table_name] = 1
        else:
            num_cols[table_name] += 1
        iden = (table_name, column_name)
        if iden in all_index_by_column:
            col.has_index = 1
            col.is_pk = all_index_by_column[iden][0]
            col.is_fk = all_index_by_column[iden][1]
            col.is_sorted = all_index_by_column[iden][2]
        else:
            col.has_index = 0
            col.is_pk = 0
            col.is_fk = 0
            col.is_sorted = 0

    for tab in table_stats:
        table_name = tab.relname
        if table_name in num_cols:
            tab.relcols = num_cols[table_name]
        else:
            tab.relcols = 0
    return database_stats


def extract_total_blocks_accessed(df: pd.DataFrame) -> int:
    rel = df[df["relname"].str.endswith("_brad_source")]
    blks_accessed = (
        rel["heap_blks_read"]
        + rel["heap_blks_hit"]
        + rel["idx_blks_read"]
        + rel["idx_blks_hit"]
    )
    return blks_accessed.sum()


def compute_blocks_accessed(raw_stats) -> int:
    pre = raw_stats.pre
    post = raw_stats.post

    pre_df = pd.DataFrame.from_records(pre.physical.data, columns=pre.physical.cols)
    post_df = pd.DataFrame.from_records(post.physical.data, columns=post.physical.cols)

    # We record data access stats before and after executing the query. The
    # difference in the counters is the number of blocks accessed by this query.
    pre_blocks = extract_total_blocks_accessed(pre_df)
    post_blocks = extract_total_blocks_accessed(post_df)
    return post_blocks - pre_blocks


def parse_plans_with_query_aurora(
    run_stats,
    min_runtime=100,
    max_runtime=200000,
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
    is_brad=True,
):
    # keep track of column statistics
    if zero_card_min_runtime is None:
        zero_card_min_runtime = min_runtime
    if explain_only:
        use_true_card = False
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

    database_stats = refine_db_stats(run_stats.database_stats, db_conn, cursor)

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

    # parse individual queries
    parsed_plans = []
    parsed_queries = []
    sql_queries = []
    avg_runtimes = []
    no_tables = []
    no_filters = []
    blocks_accessed = []
    for query_no, q in enumerate(tqdm(run_stats.query_list)):
        # either only parse explain part of query or skip entirely
        is_timeout = False
        curr_explain_only = explain_only
        # do not parse timeout queries
        if hasattr(q, "timeout") and q.timeout:
            if include_timeout:
                is_timeout = True
            else:
                continue

        alias_dict = dict()
        if not curr_explain_only and not is_timeout:
            if q.analyze_plans is None:
                print(f"parsed_query {query_no} no analyze plans")
                continue

            if len(q.analyze_plans) == 0:
                print(f"parsed_query {query_no} no analyze plans")
                continue

            # subqueries are currently not supported
            analyze_str = "".join([l[0] for l in q.verbose_plan])
            if "SubPlan" in analyze_str or "InitPlan" in analyze_str:
                print(f"parsed_query {query_no} contains SubPlan or InitPlan")
                continue

            # subquery is empty due to logical constraints
            if "->  Result  (cost=0.00..0.00 rows=0" in analyze_str:
                print(f"parsed_query {query_no} is empty")
                continue

            # check if it just initializes a plan
            if isinstance(q.analyze_plans[0][0], list):
                analyze_plan_string = "".join(l[0] for l in q.analyze_plans[0])
            else:
                analyze_plan_string = "".join(q.analyze_plans)
            if init_plan_regex.search(analyze_plan_string) is not None:
                print(f"parsed_query {query_no} init_plan_regex incorrect")
                continue

            # Extract data accessed stats.
            # There is more than one `stat_run` if we ran the query more than once.
            if hasattr(q, "data_stats") and len(q.data_stats) > 0:
                recorded_blocks = []
                for stat_run in q.data_stats:
                    recorded_blocks.append(compute_blocks_accessed(stat_run))

                if any(map(lambda b: b < 0, recorded_blocks)):
                    print(
                        f"Warning: Query {query_no} has invalid (< 0) blocks accessed stats."
                    )
                    blocks_accessed.append(0)
                else:
                    # In theory the number of blocks accessed should be
                    # deterministic, so all the values in this list should be the
                    # same. To be conservative, we record the max.
                    blocks_accessed.append(max(recorded_blocks))
            else:
                blocks_accessed.append(None)

            # compute average execution and planning times
            ex_times = []
            planning_times = []
            for analyze_plan in q.analyze_plans:
                _, ex_time, planning_time = parse_plan(
                    analyze_plan, analyze=True, parse=False
                )
                ex_times.append(ex_time)
                planning_times.append(planning_time)
            avg_runtime = sum(ex_times) / len(ex_times)

            # parse the plan as a tree
            analyze_plan, _, _ = parse_plan(
                q.analyze_plans[0], analyze=True, parse=True
            )

            # parse information contained in operator nodes (different information in verbose and analyze plan)
            analyze_plan.parse_lines_recursively(
                alias_dict=alias_dict,
                parse_baseline=False,
                parse_join_conds=False,
                is_brad=is_brad,
            )

        elif is_timeout:
            avg_runtime = max_runtime * np.uniform(1, 3)

        else:
            avg_runtime = 0

        # only explain plan (not executed)
        verbose_plan, _, _ = parse_plan(q.verbose_plan, analyze=False, parse=True)
        verbose_plan.parse_lines_recursively(
            alias_dict=alias_dict,
            parse_baseline=False,
            parse_join_conds=False,
            is_brad=is_brad,
        )
        # raw_info_plan = copy.deepcopy(verbose_plan)

        if not curr_explain_only and not is_timeout:
            # merge the plans with different information
            analyze_plan.merge_recursively(verbose_plan)

        else:
            analyze_plan = verbose_plan

        tables, filter_columns, operators = plan_statistics(analyze_plan)

        analyze_plan.parse_columns_bottom_up(
            column_id_mapping,
            partial_column_name_mapping,
            table_id_mapping,
            alias_dict=alias_dict,
        )
        analyze_plan.tables = tables
        analyze_plan.num_tables = len(tables)
        analyze_plan.plan_runtime = avg_runtime

        def augment_no_workers(p, top_no_workers=0):
            no_workers = p.plan_parameters.get("workers_planned")
            if no_workers is None:
                no_workers = top_no_workers

            p.plan_parameters["workers_planned"] = top_no_workers

            for c in p.children:
                augment_no_workers(c, top_no_workers=no_workers)

        augment_no_workers(analyze_plan)

        if not curr_explain_only:
            # check if result is None
            if analyze_plan.min_card() == 0 and not include_zero_card:
                print(f"parsed_query {query_no} has zero_card")
                continue

            elif analyze_plan.min_card() == 0 and avg_runtime < zero_card_min_runtime:
                print(f"parsed_query {query_no} has zero_card and runtime too small")
                continue

            if min_runtime is not None and avg_runtime < min_runtime:
                print(f"parsed_query {query_no} runtime too small")
                continue

            if avg_runtime > max_runtime:
                print(f"parsed_query {query_no} runtime too large")
                continue

        # parsed_query = None
        parsed_query = parse_query_from_plan(
            alias_dict,
            database_stats,
            analyze_plan,
            q.sql,
            column_id_mapping,
            table_id_mapping,
            is_explain_only=curr_explain_only,
            use_true_card=use_true_card,
            db_conn=db_conn,
            cursor=cursor,
            timeout_ms=timeout_ms,
            return_namespace=False,
            is_brad=is_brad,
            cache=cache,
        )
        if "tables" in analyze_plan:
            analyze_plan["tables"] = list(analyze_plan["tables"])
        else:
            analyze_plan["tables"] = []
        if parsed_query is not None and len(parsed_query["join_nodes"]) != 0:
            parsed_queries.append(parsed_query)
            parsed_plans.append(analyze_plan)
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
                run_kwargs=run_stats.run_kwargs,
            )
            with open(target_path, "w") as outfile:
                json.dump(parsed_runs, outfile, default=dumper)

    parsed_runs = dict(
        parsed_plans=parsed_plans,
        parsed_queries=parsed_queries,
        sql_queries=sql_queries,
        database_stats=database_stats,
        run_kwargs=run_stats.run_kwargs,
    )

    if not all(map(lambda ba: ba is None, blocks_accessed)):
        parsed_runs["blocks_accessed"] = blocks_accessed

    stats = dict(
        runtimes=str(avg_runtimes), no_tables=str(no_tables), no_filters=str(no_filters)
    )

    if query_cache_file and save_cache:
        with open(query_cache_file, "w") as outfile:
            json.dump(run_stats, outfile)

    return parsed_runs, stats
