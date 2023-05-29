# We adpated the legacy from from https://github.com/DataManagementLab/zero-shot-cost-estimation
from workloads.cross_db_benchmark.benchmark_tools.database import DatabaseSystem
from workloads.cross_db_benchmark.benchmark_tools.postgres.database_connection import (
    PostgresDatabaseConnection,
)
from workloads.cross_db_benchmark.benchmark_tools.aurora.database_connection import (
    AuroraDatabaseConnection,
)
from workloads.cross_db_benchmark.benchmark_tools.redshift.database_connection import (
    RedshiftDatabaseConnection,
)
from workloads.cross_db_benchmark.benchmark_tools.athena.database_connection import (
    AthenaDatabaseConnection,
)


def create_db_conn(database, db_name, database_conn_args, database_kwarg_dict):
    if database == DatabaseSystem.POSTGRES:
        return PostgresDatabaseConnection(
            db_name=db_name, database_kwargs=database_conn_args, **database_kwarg_dict
        )
    elif database == DatabaseSystem.AURORA:
        return AuroraDatabaseConnection(
            db_name=db_name, database_kwargs=database_conn_args
        )
    elif database == DatabaseSystem.REDSHIFT:
        return RedshiftDatabaseConnection(
            db_name=db_name, database_kwargs=database_conn_args
        )
    elif database == DatabaseSystem.ATHENA:
        return AthenaDatabaseConnection(db_name=db_name)
    else:
        raise NotImplementedError(f"Database {database} not yet supported.")


def load_database(
    data_dir,
    dataset,
    database,
    db_name,
    database_conn_args,
    database_kwarg_dict,
    force=False,
):
    db_conn = create_db_conn(database, db_name, database_conn_args, database_kwarg_dict)
    db_conn.load_database(dataset, data_dir, force=force)
