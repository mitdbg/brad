import asyncio
import redshift_connector
import redshift_connector.error as redshift_errors
import struct
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
        # TODO: Enforce connection timeouts, but not query timeouts.
        timeout_s: int,  # pylint: disable=unused-argument
    ) -> Connection:
        loop = asyncio.get_running_loop()

        def make_connection():
            kwargs = {
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "database": schema_name if schema_name is not None else "dev",
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
        # TODO: Enforce connection timeouts, but not query timeouts.
        timeout_s: int,  # pylint: disable=unused-argument
    ) -> Connection:
        kwargs = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": schema_name if schema_name is not None else "dev",
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
        self._is_closed = False

    async def cursor(self) -> Cursor:
        if self._cursor is None:
            loop = asyncio.get_running_loop()
            cursor_impl = await loop.run_in_executor(None, self._connection.cursor)
            self._cursor = RedshiftCursor(cursor_impl, self._connection)
        return self._cursor

    async def close(self) -> None:
        if self._is_closed:
            return
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._connection.close)
        self._is_closed = True

    def cursor_sync(self) -> Cursor:
        if self._cursor is None:
            self._cursor = RedshiftCursor(self._connection.cursor(), self._connection)
        return self._cursor

    def close_sync(self) -> None:
        if self._is_closed:
            return
        self._connection.close()
        self._is_closed = True

    def is_connection_lost_error(self, ex: Exception) -> bool:
        if isinstance(ex, redshift_errors.InterfaceError):
            return True
        if isinstance(ex, IndexError) or isinstance(ex, struct.error):
            # Not ideal, but this happens inside the Redshift connector
            # (probably during a Redshift restart).
            return True
        message = repr(ex)
        for phrase in _CONNECTION_LOST_PHRASES:
            if phrase in message:
                return True
        return False

    def __del__(self) -> None:
        self.close_sync()


_CONNECTION_LOST_PHRASES = [
    "server socket closed",
    "EOF occurred in violation of protocol",
]
