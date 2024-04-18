import asyncio
import pyodbc
import os
import logging
from typing import Optional

from brad.config.engine import Engine
from brad.connection.factory import ConnectionFactory, Connection, RedshiftConnection
from brad.connection.odbc_connection import OdbcConnection
from brad.config.file import ConfigFile
from brad.grpc_client import BradGrpcClient
from brad.provisioning.directory import Directory
from workload_utils.database import (
    Database,
    PyodbcDatabase,
    BradDatabase,
    DirectConnection,
)


def connect_to_db(
    args,
    worker_index: int,
    direct_engine: Optional[Engine] = None,
    directory: Optional[Directory] = None,
    disable_direct_redshift_result_cache: bool = False,
    verbose_logger: Optional[logging.Logger] = None,
) -> Database:
    if hasattr(args, "brad_direct") and args.brad_direct:
        assert direct_engine is not None
        assert args.schema_name is not None
        assert args.config_file is not None

        config = ConfigFile.load(args.config_file)
        connection_details = config.get_connection_details(direct_engine)
        if (
            direct_engine == Engine.Redshift
            and hasattr(args, "serverless_redshift")
            and args.serverless_redshift
        ):
            print("Connecting to serverless Redshift")

            def do_connect() -> Connection:
                return RedshiftConnection.connect_sync(
                    host=connection_details["serverless_endpoint"],
                    port=5439,
                    user=connection_details["user"],
                    password=connection_details["password"],
                    schema_name=args.schema_name,
                    autocommit=False,
                    timeout_s=10,
                )

        elif (
            direct_engine == Engine.Aurora
            and hasattr(args, "serverless_aurora")
            and args.serverless_aurora
        ):
            print("Connecting to serverless Aurora")

            def do_connect() -> Connection:
                # pylint: disable-next=protected-access
                cstr = ConnectionFactory._pg_aurora_odbc_connection_string(
                    connection_details["serverless_endpoint"],
                    5432,
                    connection_details,
                    args.schema_name,
                )
                return OdbcConnection.connect_sync(cstr, autocommit=False, timeout_s=10)

        else:
            if directory is None:
                directory_to_use = Directory(config)
                asyncio.run(directory_to_use.refresh())
            else:
                directory_to_use = directory

            def do_connect() -> Connection:
                return ConnectionFactory.connect_to_sync(
                    direct_engine, args.schema_name, config, directory_to_use
                )

        conn = do_connect()

        if disable_direct_redshift_result_cache and direct_engine == Engine.Redshift:
            cursor = conn.cursor_sync()
            cursor.execute_sync("SET enable_result_cache_for_session = off")

        db: Database = DirectConnection(conn, do_reconnect=do_connect)

    elif args.cstr_var is not None:
        db = PyodbcDatabase(pyodbc.connect(os.environ[args.cstr_var], autocommit=True))

    else:
        port_offset = (worker_index + args.client_offset) % args.num_front_ends
        port = args.brad_port + port_offset
        if verbose_logger is not None:
            verbose_logger.info(
                "[%d] Connecting to BRAD at %s:%d", worker_index, args.brad_host, port
            )
        brad = BradGrpcClient(args.brad_host, port)
        brad.connect()
        if verbose_logger is not None:
            verbose_logger.info("[%d] Connected to BRAD.", worker_index)
        db = BradDatabase(brad)

    return db
