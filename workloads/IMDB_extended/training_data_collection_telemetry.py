import json
from tqdm import tqdm
from workloads.IMDB_extended.gen_telemetry_workload import generate_workload
from workloads.cross_db_benchmark.benchmark_tools.athena.database_connection import AthenaDatabaseConnection
from workloads.cross_db_benchmark.benchmark_tools.redshift.database_connection import RedshiftDatabaseConnection



def duplicate_data(conn, scale=1):
    for i in range(scale):
        sql = "INSERT INTO movie_telemetry SELECT * FROM temp_telemetry;"
        res = conn.run_query_collect_statistics(sql, plain_run=True)
        if res["error"]:
            assert False, "data insert failed. DB internal error"


def execute_workload_redshift(db_conn, workload, timeout_sec, repetitions_per_query=3):
    db_conn.set_statement_timeout(timeout_sec)
    db_conn.clear_query_result_cache()
    redshift_result = []
    for i, sql_query in enumerate(tqdm(workload)):
        curr_statistics = db_conn.run_query_collect_statistics(
            sql_query,
            repetitions=repetitions_per_query,
            timeout_sec=timeout_sec,
        )
        curr_statistics.update(sql=sql_query)
        redshift_result.append(curr_statistics)
    return redshift_result


def execute_workload_athena(db_conn, workload, timeout_sec, repetitions_per_query=3):
    athena_result = []
    for i, sql_query in enumerate(tqdm(workload)):
        curr_statistics = dict()
        curr_statistics["runtimes"] = []
        for rep in range(repetitions_per_query):
            curr_out = db_conn.run_query_collect_statistics(
                sql_query,
                timeout_s=timeout_sec,
            )
            curr_statistics.update(curr_out)
            curr_statistics["runtimes"].append(curr_out["runtime"])
        curr_statistics.update(sql=sql_query)
        athena_result.append(curr_statistics)
    return athena_result


def collect_train_data(db_name, redshift_database_kwargs, athena_database_kwargs, scale=1, num_epoch=10,
                       starting_factor=1, timeout_sec=300, workload_path=None, save_path=None):
    redshitf_conn = RedshiftDatabaseConnection(db_name=db_name, database_kwargs=redshift_database_kwargs)
    athena_conn = AthenaDatabaseConnection(db_name=db_name, **athena_database_kwargs)
    if workload_path is not None:
        with open(workload_path, 'r') as f:
            workload_sql = f.readlines()
    else:
        workload_sql = None

    res = dict()
    for epoch in range(num_epoch):
        res[f"epoch_{epoch}"] = dict()
        if epoch != 0 or starting_factor == 0:
            duplicate_data(redshitf_conn, scale)
            duplicate_data(athena_conn, scale)
            if starting_factor == 0:
                starting_factor = 1
        table_stats = redshitf_conn.collect_db_statistics()
        table_stats['column_stats'] = [s for s in table_stats['column_stats'] if s['tablename'] == 'movie_telemetry']
        table_stats['table_stats'] = [{'relname': 'movie_telemetry', 'reltuples': 200000000 * (epoch+starting_factor),
                                       'relcols': 4, 'relpages': 0}]
        res[f"epoch_{epoch}"]["table_stats"] = table_stats
        if workload_sql is None:
            workload_sql = generate_workload(seed=epoch)
        redshift_result = execute_workload_redshift(redshitf_conn, workload_sql, timeout_sec)
        res[f"epoch_{epoch}"]["redshift_result"] = redshift_result
        athena_result = execute_workload_athena(redshitf_conn, workload_sql, timeout_sec)
        res[f"epoch_{epoch}"]["athena_result"] = athena_result

        if save_path:
            with open(save_path, "w") as outfile:
                json.dump(res, outfile)

    return res

