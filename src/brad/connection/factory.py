from typing import Optional

from .connection import Connection
from .redshift_connection import SyncRedshiftConnection, AsyncRedshiftConnection
from .odbc_connection import SyncOdbcConnection, AsyncOdbcConnection
from brad.config.file import ConfigFile
from brad.config.engine import Engine


class ConnectionFactory:
    @staticmethod
    async def connect(
        engine: Engine,
        schema_name: Optional[str],
        config: ConfigFile,
        autocommit: bool = True,
    ) -> Connection:
        if engine == Engine.Redshift:
            return await AsyncRedshiftConnection.connect(
                **config.get_connection_info(engine), autocommit=autocommit
            )
        else:
            return await AsyncOdbcConnection.connect(
                config.get_odbc_connection_string(engine, schema_name), autocommit
            )

    @staticmethod
    def connect_sync(
        engine: Engine,
        schema_name: Optional[str],
        config: ConfigFile,
        autocommit: bool = True,
    ) -> Connection:
        if engine == Engine.Redshift:
            return SyncRedshiftConnection.connect(
                **config.get_connection_info(engine), autocommit=autocommit
            )
        else:
            return SyncOdbcConnection.connect(
                config.get_odbc_connection_string(engine, schema_name), autocommit
            )
