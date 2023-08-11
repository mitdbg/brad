from typing import Dict, Optional

from .connection import Connection
from .odbc_connection import OdbcConnection
from .pyathena_connection import PyAthenaConnection
from .redshift_connection import RedshiftConnection
from brad.config.file import ConfigFile
from brad.config.engine import Engine
from brad.provisioning.directory import Directory


class ConnectionFactory:
    @classmethod
    async def connect_to(
        cls,
        engine: Engine,
        schema_name: Optional[str],
        config: ConfigFile,
        directory: Directory,
        autocommit: bool = True,
        aurora_read_replica: Optional[int] = None,
        timeout_s: int = 10,
    ) -> Connection:
        connection_details = config.get_connection_details(engine)
        if engine == Engine.Redshift:
            cluster = directory.redshift_cluster()
            address, port = cluster.endpoint()
            return await RedshiftConnection.connect(
                host=address,
                port=port,
                user=connection_details["user"],
                password=connection_details["password"],
                schema_name=schema_name,
                autocommit=autocommit,
                timeout_s=timeout_s,
            )
        elif engine == Engine.Aurora:
            if aurora_read_replica is None:
                address, port = directory.aurora_writer_endpoint()
            else:
                # N.B. The caller needs to specify a valid replica index.
                instance = directory.aurora_readers()[aurora_read_replica]
                address, port = instance.endpoint()
            cstr = cls._pg_aurora_odbc_connection_string(
                address, port, connection_details, schema_name
            )
            return await OdbcConnection.connect(cstr, autocommit, timeout_s)
        elif engine == Engine.Athena:
            return await PyAthenaConnection.connect(
                aws_region=connection_details["aws_region"],
                s3_output_path=connection_details["s3_output_path"],
                access_key=connection_details["access_key"],
                access_key_secret=connection_details["access_key_secret"],
                schema_name=schema_name,
            )
        else:
            raise RuntimeError("Unsupported engine: {}".format(engine))

    @classmethod
    def connect_to_sync(
        cls,
        engine: Engine,
        schema_name: Optional[str],
        config: ConfigFile,
        directory: Directory,
        autocommit: bool = True,
        aurora_read_replica: Optional[int] = None,
        timeout_s: int = 10,
    ) -> Connection:
        connection_details = config.get_connection_details(engine)
        if engine == Engine.Redshift:
            cluster = directory.redshift_cluster()
            address, port = cluster.endpoint()
            return RedshiftConnection.connect_sync(
                host=address,
                port=port,
                user=connection_details["user"],
                password=connection_details["password"],
                schema_name=schema_name,
                autocommit=autocommit,
                timeout_s=timeout_s,
            )
        elif engine == Engine.Aurora:
            instance = (
                directory.aurora_writer()
                if aurora_read_replica is None
                else directory.aurora_readers()[aurora_read_replica]
            )
            address, port = instance.endpoint()
            cstr = cls._pg_aurora_odbc_connection_string(
                address, port, connection_details, schema_name
            )
            return OdbcConnection.connect_sync(cstr, autocommit, timeout_s)
        elif engine == Engine.Athena:
            return PyAthenaConnection.connect_sync(
                aws_region=connection_details["aws_region"],
                s3_output_path=connection_details["s3_output_path"],
                access_key=connection_details["access_key"],
                access_key_secret=connection_details["access_key_secret"],
                schema_name=schema_name,
            )
        else:
            raise RuntimeError("Unsupported engine: {}".format(engine))

    @classmethod
    async def connect_to_sidecar(
        cls, schema_name: str, config: ConfigFile
    ) -> Connection:
        connection_details = config.get_sidecar_db_details()
        cstr = cls._pg_aurora_odbc_connection_string(
            connection_details["host"],
            int(connection_details["port"]),
            connection_details,
            schema_name,
        )
        return await OdbcConnection.connect(cstr, autocommit=True, timeout_s=10)

    @staticmethod
    def _pg_aurora_odbc_connection_string(
        address: str,
        port: int,
        connection_details: Dict[str, str],
        schema_name: Optional[str],
    ) -> str:
        """
        PostgreSQL-compatible Aurora connection string.
        """
        cstr = "Driver={{{}}};Server={};Port={};Uid={};Pwd={};".format(
            connection_details["odbc_driver"],
            address,
            port,
            connection_details["user"],
            connection_details["password"],
        )
        if schema_name is not None:
            cstr += "Database={};".format(schema_name)
        return cstr

    @staticmethod
    def _athena_odbc_connection_string(
        connection_details: Dict[str, str], schema_name: Optional[str]
    ) -> str:
        cstr = "Driver={{{}}};AwsRegion={};S3OutputLocation={};AuthenticationType=IAM Credentials;UID={};PWD={};".format(
            connection_details["odbc_driver"],
            connection_details["aws_region"],
            connection_details["s3_output_path"],
            connection_details["access_key"],
            connection_details["access_key_secret"],
        )
        # TODO: Restrict connections to a workgroup.
        # We do not do so right now because the bootstrap workflow does not
        # set up an Athena workgroup.
        if schema_name is not None:
            cstr += "Schema={};".format(schema_name)
        return cstr
