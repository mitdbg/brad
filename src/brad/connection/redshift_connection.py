import asyncio
import redshift_connector
from typing import Optional

from .connection import Connection
from .cursor import Cursor
from .redshift_cursor import RedshiftCursor


class RedshiftConnection(Connection):
    @classmethod
    async def connect(
        cls,
        host: str,
        user: str,
        password: str,
        schema_name: Optional[str],
        autocommit: bool,
    ) -> Connection:
        loop = asyncio.get_running_loop()

        def make_connection():
            kwargs = {
                "host": host,
                "user": user,
                "password": password,
                "database": schema_name if schema_name is not None else "dev",
            }
            return redshift_connector.connect(**kwargs)

        connection = await loop.run_in_executor(None, make_connection)
        connection.autocommit = autocommit
        return cls(connection)

    @classmethod
    def connect_sync(
        cls,
        host: str,
        user: str,
        password: str,
        schema_name: Optional[str],
        autocommit: bool,
    ) -> Connection:
        kwargs = {
            "host": host,
            "user": user,
            "password": password,
            "database": schema_name if schema_name is not None else "dev",
        }

        connection = redshift_connector.connect(**kwargs)
        connection.autocommit = autocommit
        return cls(connection)

    def __init__(self, connection_impl: redshift_connector.Connection) -> None:
        super().__init__()
        self._connection = connection_impl
        self._cursor: Optional[Cursor] = None

    async def cursor(self) -> Cursor:
        if self._cursor is None:
            loop = asyncio.get_running_loop()
            cursor_impl = await loop.run_in_executor(None, self._connection.cursor)
            self._cursor = RedshiftCursor(cursor_impl)
        return self._cursor

    async def close(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._connection.close)

    def cursor_sync(self) -> Cursor:
        if self._cursor is None:
            self._cursor = RedshiftCursor(self._connection.cursor())
        return self._cursor

    def close_sync(self) -> None:
        self._connection.close()
