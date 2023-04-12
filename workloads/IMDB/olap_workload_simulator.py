import os
import json
from typing import Optional
from workloads.IMDB.select_olap_queries import (
    select_report_queries,
    select_adhoc_queries,
    load_queries,
)
from workloads.IMDB.util import format_time_str
from workloads.IMDB.parameters import Params


def simulate_olap_one_day(param: Params, day: Optional[int] = None) -> None:
    p = param
    queries, aurora_runtime, redshift_runtime = load_queries(p.analytic_query_dir)

    if day is not None:
        adhoc_query_dir = os.path.join(p.analytic_query_dir, f"adhoc_queries_day_{day}")
    else:
        adhoc_query_dir = os.path.join(p.analytic_query_dir, "adhoc_queries")

    if os.path.exists(adhoc_query_dir) and len(os.listdir(adhoc_query_dir)) != 0:
        if not p.force:
            print(f"adhoc queries already generated for day {day} at {adhoc_query_dir}")
            return
        else:
            for file in os.listdir(adhoc_query_dir):
                os.remove(os.path.join(adhoc_query_dir, file))
    os.makedirs(adhoc_query_dir, exist_ok=True)

    num_user = p.total_num_analytic_users
    num_queries = p.num_analytic_queries_per_user
    for user in range(num_user):
        user_name = f"analytic_user_{user+1}"
        user_queries = dict()
        for hour in range(24):
            current_queries = select_adhoc_queries(
                None,
                queries,
                aurora_runtime,
                redshift_runtime,
                num_query=int(num_queries * p.num_analytic_queries_dist[hour]),
                rt_interval=p.analytic_query_rt_interval,
                aurora_timeout=p.aurora_timeout,
                redshift_timeout=p.redshift_timeout,
                return_query_idx=True,
            )
            if len(current_queries) != 0:
                exec_freq = max(int(3600 / len(current_queries)), 1)
                for i, (idx, q) in enumerate(current_queries):
                    time_in_sec = exec_freq * (i + 1) - 1
                    time_str = format_time_str(hour, time_in_sec)
                    q_name = f"{idx}_{time_str}"
                    user_queries[q_name] = q

        json_file_name = os.path.join(adhoc_query_dir, user_name)
        with open(json_file_name + ".json", "w+") as f:
            json.dump(user_queries, f)


def simulate_olap(param: Params) -> None:
    reporting_query_dir = os.path.join(param.analytic_query_dir, "reporting_queries")
    select_report_queries(
        param.analytic_query_dir,
        selected_query_rt_interval=param.reporting_query_rt_interval,
        save_dir=reporting_query_dir,
        force=param.force,
    )
    num_day = param.num_days
    if num_day == 1:
        simulate_olap_one_day(param)
    else:
        for day in range(num_day):
            simulate_olap_one_day(param, day=day + 1)
