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

    def __init__(self, s3_paths: List[str]) -> None:
        """
        NOTE: All S3 paths are relative to the extract path, specified in the
        configuration.
        """
        super().__init__()
        self._s3_paths = s3_paths

    def __repr__(self) -> str:
        return "".join(["DeleteS3Objects(<", str(len(self._s3_paths)), " objects>)"])

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        loop = asyncio.get_running_loop()
        for relative_s3_path in self._s3_paths:
            await loop.run_in_executor(
                None,
                self._delete_object_sync,
                ctx.s3_client(),
                ctx.s3_bucket(),
                "{}{}".format(ctx.s3_path(), relative_s3_path),
            )
        return self

    def _delete_object_sync(self, s3_client, s3_bucket: str, full_s3_path: str) -> None:
        s3_client.delete_object(
            Bucket=s3_bucket,
            Key=full_s3_path,
        )
