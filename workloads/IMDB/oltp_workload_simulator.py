import os
import json
from workloads.IMDB.util import format_time_str, IMDB_TABLE_SIZE, extract_columns, extract_join_keys
from workloads.IMDB.generate_oltp_queries import generate_oltp_queries, make_init_log_file


def simulate_oltp_one_day(param, day=None):
    p = param

    if day is not None:
        txn_query_dir = os.path.join(p.txn_query_dir, f"{day}")
    else:
        txn_query_dir = p.txn_query_dir

    if os.path.exists(txn_query_dir) and len(os.listdir(txn_query_dir)) != 0:
        if not p.force:
            print(f"transaction queries already generated for day {day} at {txn_query_dir}")
            return
        else:
            for file in os.listdir(txn_query_dir):
                os.remove(os.path.join(txn_query_dir, file))
    os.makedirs(txn_query_dir, exist_ok=True)

    PK_columns, all_columns = extract_columns(p.schema_file)
    all_table_name, join_keys = extract_join_keys(p.analytic_query_dir)
    if os.path.exists(p.txn_generation_ids_offset_file) and p.force:
        os.remove(p.txn_generation_ids_offset_file)
        make_init_log_file(p.txn_generation_ids_offset_file, all_table_name)
    elif not os.path.exists(p.txn_generation_ids_offset_file):
        make_init_log_file(p.txn_generation_ids_offset_file, all_table_name)

    num_user = p.total_num_txn_users
    num_queries = p.num_txn_queries_per_user

    for user in range(num_user):
        user_name = f"transaction_user_{user+1}"
        user_queries = dict()
        for hour in range(24):
            current_queries = generate_oltp_queries(num_queries=int(num_queries * p.num_txn_queries_dist[hour]),
                                                    generation_log_file=p.txn_generation_ids_offset_file,
                                                    table_insert_freq=p.table_insert_freq,
                                                    num_tuples_insert=p.num_tuples_insert_per_user,
                                                    PK_columns=PK_columns,
                                                    all_columns=all_columns,
                                                    all_tables=all_table_name,
                                                    all_join_keys=join_keys,
                                                    match_new_pk=p.match_new_pk
                                                    )
            if len(current_queries) != 0:
                exec_freq = max(int(3600 / len(current_queries)), 1)
                for i, q in enumerate(current_queries):
                    time_in_sec = exec_freq * (i + 1) - 1
                    q_name = format_time_str(hour, time_in_sec)
                    user_queries[q_name] = q

        json_file_name = os.path.join(txn_query_dir, user_name)
        with open(json_file_name + ".json", "w+") as f:
            json.dump(user_queries, f)

def simulate_oltp(param):
    num_day = param.num_days
    if num_day == 1:
        simulate_oltp_one_day(param)
    else:
        for day in range(num_day):
            simulate_oltp_one_day(param, day=day + 1)
