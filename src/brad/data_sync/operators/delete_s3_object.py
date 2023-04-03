import asyncio
import logging
from typing import List

from .operator import Operator
from brad.data_sync.execution.context import ExecutionContext

logger = logging.getLogger(__name__)


class DeleteS3Objects(Operator):
    """
    Deletes objects from S3. Used to clean up extracted data after finishing a
    sync.
    """

    def __init__(self, s3_client, s3_paths: List[str]) -> None:
        super().__init__()
        self._s3_client = s3_client
        self._s3_paths = s3_paths

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        loop = asyncio.get_running_loop()
        for s3_path in self._s3_paths:
            await loop.run_in_executor(
                None,
                self._delete_object_sync,
                ctx.s3_bucket(),
                s3_path,
            )
        return self

    def _delete_object_sync(self, s3_bucket: str, s3_path: str) -> None:
        self._s3_client.delete_object(
            Bucket=s3_bucket,
            Key=s3_path,
        )
