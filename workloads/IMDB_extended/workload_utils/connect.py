import asyncio
import pyodbc
import os
from typing import Optional

from brad.config.engine import Engine
from brad.connection.factory import ConnectionFactory
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
    args, worker_index: int, direct_engine: Optional[Engine] = None
) -> Database:
    if args.brad_direct:
        assert direct_engine is not None
        assert args.schema_name is not None
        assert args.config_file is not None

        config = ConfigFile(args.config_file)
        directory = Directory(config)
        asyncio.run(directory.refresh())
        conn = ConnectionFactory.connect_to(
            direct_engine, args.schema_name, config, directory
        )
        db: Database = DirectConnection(conn)

    elif args.cstr_var is not None:
        db = PyodbcDatabase(pyodbc.connect(os.environ[args.cstr_var], autocommit=True))

    else:
        port_offset = worker_index % args.num_front_ends
        brad = BradGrpcClient(args.brad_host, args.brad_port + port_offset)
        brad.connect()
        db = BradDatabase(brad)

    return db
