import asyncio
import redshift_connector
import redshift_connector.error as redshift_errors
from typing import Optional

from .connection import Connection, ConnectionFailed
from .cursor import Cursor
from .redshift_cursor import RedshiftCursor


class RedshiftConnection(Connection):
    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        user: str,
        password: str,
        schema_name: Optional[str],
        autocommit: bool,
        timeout_s: int,
    ) -> Connection:
        loop = asyncio.get_running_loop()

        def make_connection():
            kwargs = {
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "database": schema_name if schema_name is not None else "dev",
                "timeout": timeout_s,
            }
            return redshift_connector.connect(**kwargs)

        try:
            connection = await loop.run_in_executor(None, make_connection)
            connection.autocommit = autocommit
            return cls(connection)
        except redshift_errors.InterfaceError as ex:
            raise ConnectionFailed() from ex

    @classmethod
    def connect_sync(
        cls,
        host: str,
        port: int,
        user: str,
        password: str,
        schema_name: Optional[str],
        autocommit: bool,
        timeout_s: int,
    ) -> Connection:
        kwargs = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": schema_name if schema_name is not None else "dev",
            "timeout": timeout_s,
        }

        try:
            connection = redshift_connector.connect(**kwargs)
            connection.autocommit = autocommit
            return cls(connection)
        except redshift_errors.InterfaceError as ex:
            raise ConnectionFailed() from ex

    def __init__(self, connection_impl: redshift_connector.Connection) -> None:
        super().__init__()
        self._connection = connection_impl
        self._cursor: Optional[Cursor] = None

    async def cursor(self) -> Cursor:
        if self._cursor is None:
            loop = asyncio.get_running_loop()
            cursor_impl = await loop.run_in_executor(None, self._connection.cursor)
            self._cursor = RedshiftCursor(cursor_impl, self._connection)
        return self._cursor

    async def close(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._connection.close)

    def cursor_sync(self) -> Cursor:
        if self._cursor is None:
            self._cursor = RedshiftCursor(self._connection.cursor(), self._connection)
        return self._cursor

    def close_sync(self) -> None:
        self._connection.close()

    def is_connection_lost_error(self, ex: Exception) -> bool:
        # TODO: Determine how to check for lost connection errors.
        return False
