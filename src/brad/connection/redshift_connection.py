import asyncio
import redshift_connector
from typing import Optional

from .connection import Connection
from brad.connection.cursor import Cursor


class SyncRedshiftConnection(Connection):
    @classmethod
    def connect(
        cls,
        host: str,
        username: str,
        password: str,
        schema_name: Optional[str],
        autocommit: bool,
    ) -> None:
        kwargs = {
            "host": host,
            "username": username,
            "password": password,
            "autocommit": autocommit,
        }
        if schema_name is not None:
            kwargs["schema_name"] = schema_name

        connection = redshift_connector.connect(**kwargs)
        return cls(connection)

    def __init__(self, connection_impl: redshift_connector.Connection) -> None:
        super().__init__()
        self._connection = connection_impl
        self._cursor: Optional[Cursor] = None

    def cursor(self) -> Cursor:
        if self._cursor is None:
            self._cursor = Cursor(self._connection.cursor())
        return self._cursor

    def close(self) -> None:
        self._connection.close()


class AsyncRedshiftConnection(Connection):
    @classmethod
    async def connect(
        cls,
        host: str,
        username: str,
        password: str,
        schema_name: Optional[str],
        autocommit: bool,
    ) -> None:
        loop = asyncio.get_running_loop()

        def make_connection():
            kwargs = {
                "host": host,
                "username": username,
                "password": password,
                "autocommit": autocommit,
            }
            if schema_name is not None:
                kwargs["schema_name"] = schema_name
            return redshift_connector.connect(**kwargs)

        connection = await loop.run_in_executor(None, make_connection)
        return cls(connection)

    def __init__(self, connection_impl: redshift_connector.Connection) -> None:
        self._connection = connection_impl
        self._cursor: Optional[Cursor] = None

    async def cursor(self) -> Cursor:
        if self._cursor is None:
            loop = asyncio.get_running_loop()
            cursor_impl = await loop.run_in_executor(None, self._connection.cursor)
            self._cursor = Cursor(cursor_impl)
        return self._cursor

    async def close(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._connection.close)
