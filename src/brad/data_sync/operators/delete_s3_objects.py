import asyncio
import logging
from typing import List

from .operator import Operator
from brad.data_sync.execution.context import ExecutionContext

logger = logging.getLogger(__name__)


class DeleteS3Objects(Operator):
    """
    Deletes objects from S3. Used to clean up extracted data after finishing a
    sync or a table movement.
    """

    def __init__(self, s3_paths: List[str], paths_are_relative: bool = True) -> None:
        """
        NOTE: All S3 paths can be relative to the extract path, specified in the
        configuration.
        """
        super().__init__()
        self._s3_paths = s3_paths
        self._paths_are_relative = paths_are_relative

    def __repr__(self) -> str:
        return "".join(["DeleteS3Objects(<", str(len(self._s3_paths)), " objects>)"])

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        loop = asyncio.get_running_loop()
        for s3_path in self._s3_paths:
            await loop.run_in_executor(
                None,
                self._delete_object_sync,
                ctx.s3_client(),
                ctx.s3_bucket(),
                (
                    "{}{}".format(ctx.s3_path(), s3_path)
                    if self._paths_are_relative
                    else s3_path
                ),
            )
        return self

    def _delete_object_sync(self, s3_client, s3_bucket: str, full_s3_path: str) -> None:
        s3_client.delete_object(
            Bucket=s3_bucket,
            Key=full_s3_path,
        )
        logger.debug("Deleted S3 object: %s", full_s3_path)
