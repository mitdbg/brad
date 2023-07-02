from typing import Dict, Optional

from .connection import Connection
from .redshift_connection import RedshiftConnection
from .odbc_connection import OdbcConnection
from brad.config.file import ConfigFile
from brad.config.engine import Engine


class ConnectionFactory:
    @classmethod
    async def connect_to(
        cls,
        engine: Engine,
        schema_name: Optional[str],
        config: ConfigFile,
        autocommit: bool = True,
    ) -> Connection:
        connection_details = config.get_connection_details(engine)
        if engine == Engine.Redshift:
            return await RedshiftConnection.connect(
                host=connection_details["host"],
                user=connection_details["user"],
                password=connection_details["password"],
                schema_name=schema_name,
                autocommit=autocommit,
            )
        else:
            if engine == Engine.Aurora:
                cstr = cls._aurora_odbc_connection_string(
                    connection_details, schema_name
                )
            elif engine == Engine.Athena:
                cstr = cls._athena_odbc_connection_string(
                    connection_details, schema_name
                )
            else:
                raise RuntimeError("Unsupported engine: {}".format(engine))

            return await OdbcConnection.connect(cstr, autocommit)

    @classmethod
    def connect_to_sync(
        cls,
        engine: Engine,
        schema_name: Optional[str],
        config: ConfigFile,
        autocommit: bool = True,
    ) -> Connection:
        connection_details = config.get_connection_details(engine)
        if engine == Engine.Redshift:
            return RedshiftConnection.connect_sync(
                host=connection_details["host"],
                user=connection_details["user"],
                password=connection_details["password"],
                schema_name=schema_name,
                autocommit=autocommit,
            )
        else:
            if engine == Engine.Aurora:
                cstr = cls._aurora_odbc_connection_string(
                    connection_details, schema_name
                )
            elif engine == Engine.Athena:
                cstr = cls._athena_odbc_connection_string(
                    connection_details, schema_name
                )
            else:
                raise RuntimeError("Unsupported engine: {}".format(engine))

            return OdbcConnection.connect_sync(cstr, autocommit)

    @staticmethod
    def _aurora_odbc_connection_string(
        connection_details: Dict[str, str], schema_name: Optional[str]
    ) -> str:
        cstr = "Driver={{{}}};Server={};Port={};Uid={};Pwd={};".format(
            connection_details["odbc_driver"],
            connection_details["host"],
            connection_details["port"],
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
