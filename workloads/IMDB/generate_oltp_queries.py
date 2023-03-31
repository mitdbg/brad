import os
import numpy as np
import json
from workloads.IMDB.util import IMDB_TABLE_SIZE


def make_init_log_file(txn_generation_ids_offset_file, all_table_name):
    table_ids_offset = dict()
    for table in all_table_name:
        table_ids_offset[table] = 1
    with open(txn_generation_ids_offset_file, "w+") as f:
        json.dump(table_ids_offset, f)


def generate_oltp_queries(num_queries, generation_log_file, table_insert_freq, num_tuples_insert,
                          PK_columns, all_columns, all_tables, all_join_keys=None, match_new_pk=False):

    if num_queries == 0:
        return []

    with open(generation_log_file, "r+") as f:
        table_ids_offset = json.load(f)

    all_queries = []
    for i in range(num_queries):
        joint_txn_sql = ""
        for table in all_tables:
            num_tuples_insert_per_table = int(num_tuples_insert * table_insert_freq[table])
            if num_tuples_insert_per_table == 0:
                continue
            else:
                start_id = table_ids_offset[table]
                end_id = table_ids_offset[table] + num_tuples_insert_per_table
                table_pk = PK_columns[table]
                if end_id <= IMDB_TABLE_SIZE[table] + 1:
                    where_clause = f"{table_pk} >= {start_id} AND {table_pk} < {end_id}"
                else:
                    end_id = end_id % IMDB_TABLE_SIZE[table]
                    where_clause = f"({table_pk} >= {start_id} AND {table_pk} < {IMDB_TABLE_SIZE[table] + 1}) " \
                                   f"OR ({table_pk} < {end_id})"
                table_ids_offset[table] = end_id
                non_pk_columns = [col for col in all_columns[table] if col != table_pk]
                column_clause = ", ".join(non_pk_columns)
                select_clause = f"SELECT {column_clause} FROM {table} WHERE {where_clause}"
                if match_new_pk:
                    raise NotImplementedError
                txn_sql = f"""INSERT INTO {table} ({column_clause}) {select_clause};\n"""
                joint_txn_sql += txn_sql
        all_queries.append(joint_txn_sql + "COMMIT;")

    with open(generation_log_file, "w+") as f:
        json.dump(table_ids_offset, f)
    return all_queries
