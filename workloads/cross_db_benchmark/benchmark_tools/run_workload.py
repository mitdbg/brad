# We adapted the legacy from from https://github.com/DataManagementLab/zero-shot-cost-estimation
from workloads.cross_db_benchmark.benchmark_tools.database import DatabaseSystem
from workloads.cross_db_benchmark.benchmark_tools.postgres.run_workload import (
    run_pg_workload,
)
from workloads.cross_db_benchmark.benchmark_tools.aurora.run_workload import (
    run_aurora_workload,
    re_execute_query_with_no_result,
)
from workloads.cross_db_benchmark.benchmark_tools.redshift.run_workload import (
    run_redshift_workload,
)
from workloads.cross_db_benchmark.benchmark_tools.athena.run_workload import (
    run_athena_workload,
)


def run_workload(
    workload_path,
    database,
    db_name,
    database_conn_args,
    database_kwarg_dict,
    target_path,
    run_kwargs,
    repetitions_per_query,
    timeout_sec,
    hints=None,
    with_indexes=False,
    cap_workload=None,
    min_runtime=100,
    re_execute_query=False,
):
    if database == DatabaseSystem.POSTGRES:
        run_pg_workload(
            workload_path,
            database,
            db_name,
            database_conn_args,
            database_kwarg_dict,
            target_path,
            run_kwargs,
            repetitions_per_query,
            timeout_sec,
            hints=hints,
            with_indexes=with_indexes,
            cap_workload=cap_workload,
            min_runtime=min_runtime,
        )
    elif database == DatabaseSystem.AURORA:
        if re_execute_query:
            re_execute_query_with_no_result(
                workload_path, database_conn_args, database_kwarg_dict
            )
        else:
            run_aurora_workload(
                workload_path,
                database,
                db_name,
                database_conn_args,
                database_kwarg_dict,
                target_path,
                run_kwargs,
                repetitions_per_query,
                timeout_sec,
                cap_workload=cap_workload,
            )
    elif database == DatabaseSystem.REDSHIFT:
        run_redshift_workload(
            workload_path,
            database,
            db_name,
            database_conn_args,
            database_kwarg_dict,
            target_path,
            run_kwargs,
            repetitions_per_query,
            timeout_sec,
            cap_workload=cap_workload,
        )
    elif database == DatabaseSystem.ATHENA:
        run_athena_workload(
            workload_path,
            database,
            db_name,
            target_path,
            run_kwargs,
            timeout_sec,
            cap_workload=cap_workload,
        )
    else:
        raise NotImplementedError
