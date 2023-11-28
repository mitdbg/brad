import copy
import json

from tqdm import tqdm
import argparse
from workloads.IMDB_extended.gen_telemetry_workload import generate_workload
from workloads.cross_db_benchmark.benchmark_tools.athena.database_connection import (
    AthenaDatabaseConnection,
)
from workloads.cross_db_benchmark.benchmark_tools.redshift.database_connection import (
    RedshiftDatabaseConnection,
)
from workloads.cross_db_benchmark.benchmark_tools.utils import dumper


def reset_data_athena(conn):
    res = conn.run_query_collect_statistics(
        "DROP TABLE movie_telemetry;", plain_run=True
    )
    if res["error"]:
        assert False, "data insert failed. DB internal error"
    create_table_sql = """CREATE TABLE
                            movie_telemetry (ip string, timestamp string, movie_id int, event_id int)
                            LOCATION 's3://imdb-large-data-brad/telemetry/'
                            TBLPROPERTIES ( 'table_type' ='ICEBERG' );
                            """
    res = conn.run_query_collect_statistics(create_table_sql, plain_run=True)
    if res["error"]:
        assert False, "data insert failed. DB internal error"
    res = conn.run_query_collect_statistics(
        "INSERT INTO movie_telemetry SELECT * FROM temp_telemetry;", plain_run=True
    )
    if res["error"]:
        assert False, "data insert failed. DB internal error"


def reset_data_redshift(conn):
    _ = conn.run_query_collect_statistics("DROP TABLE movie_telemetry;", plain_run=True)
    create_table_sql = "CREATE TABLE movie_telemetry (ip character varying, timestamp TIMESTAMP, movie_id int, event_id int);"
    _ = conn.run_query_collect_statistics(create_table_sql, plain_run=True)
    _ = conn.run_query_collect_statistics(
        "INSERT INTO movie_telemetry SELECT * FROM temp_telemetry;", plain_run=True
    )


def duplicate_data(conn, scale=1):
    for _ in range(scale):
        sql = "INSERT INTO movie_telemetry SELECT * FROM temp_telemetry;"
        res = conn.run_query_collect_statistics(sql, plain_run=True)
        if "error" in res and res["error"]:
            assert False, "data insert failed. DB internal error"


def execute_workload_redshift(db_conn, workload, timeout_sec, repetitions_per_query=3):
    db_conn.set_statement_timeout(timeout_sec)
    db_conn.clear_query_result_cache()
    redshift_result = []
    for sql_query in tqdm(workload):
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
    for sql_query in tqdm(workload):
        curr_statistics = dict()
        curr_statistics["runtimes"] = []
        for _ in range(repetitions_per_query):
            curr_out = db_conn.run_query_collect_statistics(
                sql_query,
                timeout_s=timeout_sec,
            )
            curr_statistics.update(curr_out)
            curr_statistics["runtimes"].append(curr_out["runtime"])
        curr_statistics.update(sql=sql_query)
        athena_result.append(curr_statistics)
    return athena_result


def collect_train_data(
    db_name,
    redshift_database_kwargs,
    athena_database_kwargs,
    re_start=False,
    scale=1,
    num_epoch=10,
    starting_factor=1,
    timeout_sec=300,
    repetitions_per_query=3,
    workload_path=None,
    num_queries_per_template=10,
    save_path=None,
):
    redshitf_conn = RedshiftDatabaseConnection(
        db_name=db_name, database_kwargs=redshift_database_kwargs
    )
    athena_conn = AthenaDatabaseConnection(db_name=db_name, **athena_database_kwargs)
    if workload_path is not None:
        with open(workload_path, "r", encoding="utf-8") as f:
            workload_sql = f.readlines()
    else:
        workload_sql = None

    res = dict()
    if re_start:
        reset_data_redshift(redshitf_conn)
        reset_data_athena(athena_conn)
    for epoch in range(num_epoch):
        res[f"epoch_{epoch}"] = dict()
        if epoch != 0 or starting_factor == 0:
            duplicate_data(redshitf_conn, scale)
            duplicate_data(athena_conn, scale)
            if starting_factor == 0:
                starting_factor = 1
        table_stats = redshitf_conn.collect_db_statistics()
        table_stats["column_stats"] = [
            s
            for s in table_stats["column_stats"]
            if s["tablename"] == "movie_telemetry"
        ]
        table_stats["table_stats"] = [
            {
                "relname": "movie_telemetry",
                "reltuples": 200000000 * (epoch + starting_factor),
                "relcols": 4,
                "relpages": 0,
            }
        ]
        res[f"epoch_{epoch}"]["table_stats"] = table_stats
        if workload_sql is None:
            workload_sql = generate_workload(
                seed=epoch, num_queries_per_template=num_queries_per_template
            )
        redshift_result = execute_workload_redshift(
            redshitf_conn, workload_sql, timeout_sec, repetitions_per_query
        )
        res[f"epoch_{epoch}"]["redshift_result"] = redshift_result
        athena_result = execute_workload_athena(
            athena_conn, workload_sql, timeout_sec, repetitions_per_query
        )
        res[f"epoch_{epoch}"]["athena_result"] = athena_result

        if save_path:
            with open(save_path, "w", encoding="utf-8") as outfile:
                json.dump(res, outfile)
    return res


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_name", default="imdb", type=str)

    parser.add_argument("--host", type=str)
    parser.add_argument("--port", default="5439", type=str)
    parser.add_argument("--user", type=str)
    parser.add_argument("--sslrootcert", default="SSLCERTIFICATE", type=str)
    parser.add_argument("--password", type=str)

    parser.add_argument("--aws_access_key", type=str)
    parser.add_argument("--aws_secret_key", type=str)
    parser.add_argument("--s3_staging_dir", type=str)
    parser.add_argument("--aws_region", default="us-east-1", type=str)

    parser.add_argument("--re_start", action="store_true")
    parser.add_argument("--scale", default=1, type=int)
    parser.add_argument("--num_epoch", default=10, type=int)
    parser.add_argument("--starting_factor", default=1, type=int)
    parser.add_argument("--repetitions_per_query", default=3, type=int)
    parser.add_argument("--timeout_sec", default=300, type=int)
    parser.add_argument("--workload_path", default=None, type=str)
    parser.add_argument("--num_queries_per_template", default=10, type=int)
    parser.add_argument("--save_path", default=None, type=str)

    args = parser.parse_args()

    r_database_kwargs = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "sslrootcert": args.sslrootcert,
    }
    a_database_kwargs = {
        "aws_access_key": args.aws_access_key,
        "aws_secret_key": args.aws_secret_key,
        "s3_staging_dir": args.s3_staging_dir,
        "aws_region": args.aws_region,
    }

    _ = collect_train_data(
        args.db_name,
        r_database_kwargs,
        a_database_kwargs,
        args.re_start,
        args.scale,
        args.num_epoch,
        args.starting_factor,
        args.timeout_sec,
        args.repetitions_per_query,
        args.workload_path,
        args.num_queries_per_template,
        args.save_path,
    )


def simulate_query_on_larger_scale(
    old_parsed_queries, current_scale, target_scale, target_path=None
):
    scale_factor = target_scale / current_scale
    parsed_queries = copy.deepcopy(old_parsed_queries)
    db_stats = parsed_queries["database_stats"]
    for column_stats in db_stats["column_stats"]:
        column_stats["table_size"] = int(column_stats["table_size"] * scale_factor)
    for table_stats in db_stats["table_stats"]:
        table_stats["reltuples"] = int(table_stats["reltuples"] * scale_factor)
    parsed_queries["database_stats"] = db_stats
    parsed_queries["sql_queries"] = old_parsed_queries["sql_queries"]
    parsed_queries["run_kwargs"] = old_parsed_queries["run_kwargs"]
    parsed_queries["skipped"] = old_parsed_queries["skipped"]
    parsed_queries["parsed_plans"] = []

    for q in parsed_queries["parsed_queries"]:
        for table_num in q["scan_nodes"]:
            scan_node_param = q["scan_nodes"][table_num]["plan_parameters"]
            scan_node_param["est_card"] = int(
                scan_node_param["est_card"] * scale_factor
            )
            scan_node_param["act_card"] = int(
                scan_node_param["act_card"] * scale_factor
            )
            scan_node_param["est_children_card"] = int(
                scan_node_param["est_children_card"] * scale_factor
            )
            scan_node_param["act_children_card"] = int(
                scan_node_param["act_children_card"] * scale_factor
            )

    if target_path is not None:
        with open(
            target_path + f"_epoch_{target_scale}.json", "w", encoding="UTF-8"
        ) as f:
            json.dump(parsed_queries, f, default=dumper)
    return parsed_queries
