import re
import os
import numpy as np


IMDB_TABLE_SIZE = {
    "aka_name": 901343,
    "aka_title": 361472,
    "cast_info": 36244344,
    "char_name": 3140339,
    "comp_cast_type": 4,
    "company_name": 234997,
    "company_type": 4,
    "complete_cast": 135086,
    "info_type": 113,
    "keyword": 134170,
    "kind_type": 7,
    "link_type": 18,
    "movie_companies": 2609129,
    "movie_info": 14835720,
    "movie_info_idx": 1380035,
    "movie_keyword": 4523930,
    "movie_link": 29997,
    "name": 4167491,
    "person_info": 2963664,
    "role_type": 12,
    "title": 2528312,
}


def load_queries(query_dir):
    query_path = os.path.join(query_dir, "all_queries.sql")
    aurora_rt_path = os.path.join(query_dir, "all_queries_aurora_runtime.npy")
    redshift_rt_path = os.path.join(query_dir, "all_queries_redshift_runtime.npy")

    with open(query_path, "r") as f:
        queries = f.read()
    queries = queries.split("\n")[:-1]
    aurora_runtime = np.load(aurora_rt_path)
    redshift_runtime = np.load(redshift_rt_path)
    return queries, aurora_runtime, redshift_runtime


def get_table_names(sql, all_tables=None, return_join=False):
    table_names = set()
    from_clause = sql.split(" FROM ")[-1].split(" WHERE ")[0]
    join_cond_pat = re.compile(
        r"""
        \"
        (\w+)  # 1st table
        \"
        \.     # the dot "."
        \"
        (\w+)  # 1st table column
        \"
        \s*    # optional whitespace
        =      # the equal sign "="
        \s*    # optional whitespace
        \"
        (\w+)  # 2nd table
        \"
        \.     # the dot "."
        \"
        (\w+)  # 2nd table column
        \"
        """,
        re.VERBOSE,
    )
    join_conds = join_cond_pat.findall(sql)
    for t1, k1, t2, k2 in join_conds:
        if all_tables:
            all_tables.add(t1)
            all_tables.add(t2)
        table_names.add(t1)
        table_names.add(t2)
    if return_join:
        return table_names, join_conds
    return table_names


def format_time_str(hour, time_in_sec):
    assert 0 <= time_in_sec < 3600
    hour_str = f"0{hour}" if hour < 10 else str(hour)
    minute = int(time_in_sec / 60)
    second = time_in_sec % 60
    minute_str = f"0{minute}" if minute < 10 else str(minute)
    second_str = f"0{second}" if second < 10 else str(second)
    return f"{hour_str}:{minute_str}:{second_str}"


def extract_columns(pg_schema_path):
    PK_columns = dict()  # In IMDB workload, PK is always id
    all_columns = dict()
    with open(pg_schema_path, "r") as file:
        pg_schema = file.read()
    if "DROP TABLE IF EXISTS" in pg_schema:
        pg_schema = pg_schema.split("DROP TABLE IF EXISTS ")
    elif "drop table if exists" in pg_schema:
        pg_schema = pg_schema.split("drop table if exists ")
    else:
        raise NotImplementedError
    for table_def in pg_schema:
        if table_def.startswith('"'):
            table_name = table_def.split("\n")[0].strip('"; ')
            PK_columns[table_name] = set()
            all_columns[table_name] = []
            # first way of specifying PK
            pk_regex = re.finditer('PRIMARY KEY \(("\S+"(,|, )?)+\)', table_def)
            for matched_pk in pk_regex:
                pk_columns = [c.strip('"') for c in matched_pk.groups()[0].split(",")]
                for c in pk_columns:
                    PK_columns[table_name].add(c)
            for l in table_def.split("\n"):
                if "PRIMARY KEY," in l:
                    pk_alt = l.strip().split(" ")[0].strip('"')
                    PK_columns[table_name].add(pk_alt)
            assert len(PK_columns[table_name]) == 1, (
                f"Multiple primary keys {PK_columns[table_name]} "
                f"exist for table {table_name}"
            )
            PK_columns[table_name] = list(PK_columns[table_name])[0]

            for line in table_def.strip().split("\n"):
                line = line.strip()
                if "CREATE TABLE" in line:
                    continue
                elif " " not in line:
                    continue
                column = line.split(" ")[0]
                all_columns[table_name].append(column)

    return PK_columns, all_columns


def extract_join_keys(query_dir):
    queries, _, _ = load_queries(query_dir)
    all_tables = set()
    all_join_conds = dict()
    for q in queries:
        table_names, join_conds = get_table_names(q, return_join=True)
        all_tables = all_tables.union(table_names)
        for t1, k1, t2, k2 in join_conds:
            if t1 not in all_join_conds:
                all_join_conds[t1] = set()
            all_join_conds[t1].add((k1, t2))
            if t2 not in all_join_conds:
                all_join_conds[t2] = set()
            all_join_conds[t2].add((k2, t1))
    return all_tables, all_join_conds
