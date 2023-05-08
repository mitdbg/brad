import os
import json
from datetime import timedelta
from typing import Tuple, Mapping

from workloads.runner.query import Query
from workloads.runner.workload import Workload
from workloads.runner.schedule import Once, Repeat
from workloads.runner.time import get_current_time, get_time_delta
from workloads.runner.user import User


def read_test_run_workload() -> Workload:
    current_time = get_current_time()
    interval = timedelta(seconds=1)

    workload = Workload.combine(
        [
            Workload.serial(
                [
                    Query(
                        f"SELECT COUNT(*) FROM info_type WHERE id > {i};",
                        Once(at=current_time + 0.47 * i * interval),
                    )
                    for i in range(10)
                ],
                user=User.with_label("Once"),
            ),
            Workload.serial(
                [
                    Query(
                        """SELECT MAX("title"."episode_nr" + "movie_companies"."movie_id") 
                           as agg_0 FROM "company_type" LEFT OUTER JOIN "movie_companies" 
                           ON "company_type"."id" = "movie_companies"."company_type_id" LEFT OUTER JOIN "title" 
                           ON "movie_companies"."movie_id" = "title"."id" LEFT OUTER JOIN "company_name" ON 
                           "movie_companies"."company_id" = "company_name"."id"  WHERE "title"."title" NOT LIKE '%t%he%' 
                           AND "movie_companies"."note" NOT LIKE '%media)%' AND ("company_type"."kind" NOT LIKE 
                           '%companie%s%' OR "company_type"."id" 
                           BETWEEN 2 AND 3 OR "company_type"."kind" LIKE '%companies%') AND 
                           "company_name"."country_code" NOT LIKE '%[us]%';""",
                        Repeat.starting_now(
                            interval=timedelta(seconds=20), num_repeat=5
                        ),
                    ),
                ],
                user=User.with_label("Repeat"),
            ),
        ]
    )
    return workload


def read_workload_from_file(
    txn_query_dir: str,
    analytic_query_dir: str,
    num_txn_users: int,
    num_analytic_users: int,
) -> Tuple[Mapping[str, Mapping[str, str]], Mapping[str, Mapping[str, str]], list[str]]:
    txn_users_queries = dict()
    analytic_users_queries = dict()
    reporting_queries = []

    for i in range(num_txn_users):
        user_name = f"txn_user_{i + 1}"
        query_file = os.path.join(txn_query_dir, f"transaction_user_{i + 1}.json")
        if os.path.exists(query_file):
            with open(query_file, "r") as f:
                txn_users_queries[user_name] = json.load(f)

    for i in range(num_analytic_users):
        user_name = f"num_analytic_user_{i + 1}"
        query_file = os.path.join(
            analytic_query_dir, "adhoc_queries", f"analytic_user_{i + 1}.json"
        )
        if os.path.exists(query_file):
            with open(query_file, "r") as f:
                analytic_users_queries[user_name] = json.load(f)

    for file in os.listdir(os.path.join(analytic_query_dir, "reporting_queries")):
        if file.endswith(".sql"):
            with open(
                os.path.join(analytic_query_dir, "reporting_queries", file), "r"
            ) as f:
                sql = f.read()
            reporting_queries.append(sql)
    return txn_users_queries, analytic_users_queries, reporting_queries


def make_imdb_workload(
    txn_query_dir: str,
    analytic_query_dir: str,
    num_txn_users: int,
    num_analytic_users: int,
    reporting_time_window: Tuple[str, str],
    num_days: int = 1,
    start_time: str = "00:00:00",
    auto_commit: bool = True,
    fast_forward_factor: float = 1.0,
) -> Workload:
    (
        txn_users_queries,
        analytic_users_queries,
        reporting_queries,
    ) = read_workload_from_file(
        txn_query_dir, analytic_query_dir, num_txn_users, num_analytic_users
    )

    current_time = get_current_time()
    workloads = []
    for txn_user in txn_users_queries:
        queries = []
        # dictionary with key = string of execution time: a string of set of insert sql statement
        user_queries = txn_users_queries[txn_user]
        for time_of_exec in user_queries:
            interval = get_time_delta(start_time, time_of_exec)
            exec_time = Once(at=current_time + interval / fast_forward_factor)
            for sql in user_queries[time_of_exec].split(";\n"):
                if not auto_commit or sql != "COMMIT;":
                    query = Query(sql, exec_time)
                    queries.append(query)
        workloads.append(Workload.serial(queries, user=User.with_label(txn_user)))

    for analytic_user in analytic_users_queries:
        queries = []
        # dictionary with key = string of execution time: a string of set of insert sql statement
        user_queries = txn_users_queries[analytic_user]

        for time_of_exec in user_queries:
            time_of_exec = time_of_exec.split("_")[-1]
            interval = get_time_delta(start_time, time_of_exec)
            exec_time = Once(at=current_time + interval / fast_forward_factor)
            sql = user_queries[time_of_exec]
            query = Query(sql, exec_time)
            queries.append(query)
        workloads.append(Workload.serial(queries, user=User.with_label(analytic_user)))

    reporting_start_time = get_time_delta(start_time, reporting_time_window[0])

    one_day_in_sec = get_time_delta("00:00:00", "24:00:00")
    queries = []
    for sql in reporting_queries:
        query = Query(
            sql,
            Repeat.starting_at(
                interval=one_day_in_sec / fast_forward_factor,
                start_time=current_time + reporting_start_time,
                num_repeat=num_days,
            ),
        )
        queries.append(query)
    workloads.append(Workload.serial(queries, user=User.with_label("reporting")))

    return Workload.combine(workloads)
