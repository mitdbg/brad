import asyncio
import pyathena
import pyathena.connection
import boto3
from typing import Optional

from .connection import Connection
from .cursor import Cursor
from .pyathena_cursor import PyAthenaCursor


class PyAthenaConnection(Connection):
    @classmethod
    async def connect(
        cls,
        aws_region: str,
        s3_output_path: str,
        access_key: str,
        access_key_secret: str,
        schema_name: Optional[str],
    ) -> Connection:
        return cls.connect_sync(
            aws_region, s3_output_path, access_key, access_key_secret, schema_name
        )

    @classmethod
    def connect_sync(
        cls,
        aws_region: str,
        s3_output_path: str,
        access_key: str,
        access_key_secret: str,
        schema_name: Optional[str],
    ) -> Connection:
        kwargs = {
            "region_name": aws_region,
            "s3_staging_dir": s3_output_path,
            "session": boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=access_key_secret,
            ),
        }
        if schema_name is not None:
            kwargs["schema_name"] = schema_name
        return cls(pyathena.connect(**kwargs))

    def __init__(self, connection_impl: pyathena.connection.Connection) -> None:
        super().__init__()
        self._connection = connection_impl
        self._cursor: Optional[Cursor] = None

    async def cursor(self) -> Cursor:
        if self._cursor is None:
            loop = asyncio.get_running_loop()
            cursor_impl = await loop.run_in_executor(None, self._connection.cursor)
            self._cursor = PyAthenaCursor(cursor_impl)  # type: ignore
        return self._cursor

    async def close(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._connection.close)

    def cursor_sync(self) -> Cursor:
        if self._cursor is None:
            self._cursor = PyAthenaCursor(self._connection.cursor())  # type: ignore
        return self._cursor

    def close_sync(self) -> None:
        self._connection.close()
