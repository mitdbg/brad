import asyncio
import boto3
from typing import Any, Dict, List, Optional, Tuple

from brad.config.file import ConfigFile


class Directory:
    """
    Stores low-level AWS-specific metadata about the provisioned engines (e.g.,
    endpoints, instance IDs, roles, etc.). This information is usually needed
    for access to AWS APIs and so should only be queried by classes that require
    low level API access.

    NOTE: This class currently assumes that the underlying engines are
    provisioned, even if the blueprint marks them as "turned off". The engines
    can be paused, but they must exist in AWS.
    """

    def __init__(self, config: ConfigFile) -> None:
        self._config = config
        self._rds = boto3.client(
            "rds",
            aws_access_key_id=self._config.aws_access_key,
            aws_secret_access_key=self._config.aws_access_key_secret,
        )
        self._redshift = boto3.client(
            "redshift",
            aws_access_key_id=self._config.aws_access_key,
            aws_secret_access_key=self._config.aws_access_key_secret,
        )

        self._aurora_writer: Optional["AuroraInstanceMetadata"] = None
        self._aurora_readers: List["AuroraInstanceMetadata"] = []
        self._redshift_cluster: Optional["RedshiftClusterMetadata"] = None

    def aurora_writer(self) -> "AuroraInstanceMetadata":
        assert self._aurora_writer is not None
        return self._aurora_writer

    def aurora_readers(self) -> List["AuroraInstanceMetadata"]:
        return self._aurora_readers

    def redshift_cluster(self) -> "RedshiftClusterMetadata":
        assert self._redshift_cluster is not None
        return self._redshift_cluster

    async def refresh(self) -> None:
        aurora_writer, aurora_readers = await self._refresh_aurora()
        redshift = await self._refresh_redshift()

        self._aurora_writer = aurora_writer
        self._aurora_readers.clear()
        self._aurora_readers.extend(aurora_readers)
        self._redshift_cluster = redshift

    async def _refresh_aurora(
        self,
    ) -> Tuple["AuroraInstanceMetadata", List["AuroraInstanceMetadata"]]:
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(None, self._call_describe_aurora_cluster)

        new_writer: Optional[AuroraInstanceMetadata] = None
        new_readers: List[AuroraInstanceMetadata] = []

        for cluster_info in response["DBClusters"]:
            for instance_info in cluster_info["DBClusterMembers"]:
                instance_id = instance_info["DBInstanceIdentifier"]
                is_writer = instance_info["IsClusterWriter"]

                if is_writer:
                    assert new_writer is None
                    new_writer = await self._refresh_aurora_instance(instance_id)
                else:
                    new_readers.append(await self._refresh_aurora_instance(instance_id))

        assert new_writer is not None
        new_readers.sort()

        return (new_writer, new_readers)

    async def _refresh_aurora_instance(
        self, instance_id: str
    ) -> "AuroraInstanceMetadata":
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None, self._call_describe_aurora_instance, instance_id
        )
        # NOTE: This might also include instances being deleted (?).
        instance_data = response["DBInstances"][0]
        kwargs = {
            "instance_id": instance_id,
            "resource_id": instance_data["DbiResourceId"],
            "endpoint_address": instance_data["Endpoint"]["Address"],
            "endpoint_port": instance_data["Endpoint"]["Port"],
        }
        return AuroraInstanceMetadata(**kwargs)

    async def _refresh_redshift(self) -> "RedshiftClusterMetadata":
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None, self._call_describe_redshift_cluster
        )

        cluster = response["Clusters"][0]
        kwargs = {
            "endpoint_address": cluster["Endpoint"]["Address"],
            "endpoint_port": cluster["Endpoint"]["Port"],
        }
        return RedshiftClusterMetadata(**kwargs)

    def _call_describe_aurora_cluster(self) -> Dict[Any, Any]:
        return self._rds.describe_db_clusters(
            DBClusterIdentifier=self._config.aurora_cluster_id
        )

    def _call_describe_aurora_instance(self, instance_id: str) -> Dict[Any, Any]:
        return self._rds.describe_db_instances(
            DBInstanceIdentifier=instance_id,
        )

    def _call_describe_redshift_cluster(self) -> Dict[Any, Any]:
        return self._redshift.describe_clusters(
            ClusterIdentifier=self._config.redshift_cluster_id
        )


class AuroraInstanceMetadata:
    def __init__(
        self,
        instance_id: str,
        resource_id: str,
        endpoint_address: str,
        endpoint_port: int,
    ) -> None:
        self._instance_id = instance_id
        self._resource_id = resource_id
        self._endpoint_address = endpoint_address
        self._endpoint_port = endpoint_port

    def instance_id(self) -> str:
        return self._instance_id

    def resource_id(self) -> str:
        return self._resource_id

    def endpoint(self) -> Tuple[str, int]:
        return (self._endpoint_address, self._endpoint_port)

    def __lt__(self, other: "AuroraInstanceMetadata") -> bool:
        return self.instance_id() < other.instance_id()


class RedshiftClusterMetadata:
    def __init__(self, endpoint_address: str, endpoint_port: int) -> None:
        self._endpoint_address = endpoint_address
        self._endpoint_port = endpoint_port

    def endpoint(self) -> Tuple[str, int]:
        return (self._endpoint_address, self._endpoint_port)
