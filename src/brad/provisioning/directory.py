import asyncio
import boto3
import logging
from typing import Any, Dict, List, Optional, Tuple

from .rds_status import RdsStatus
from .redshift_status import RedshiftAvailabilityStatus
from brad.config.file import ConfigFile

logger = logging.getLogger(__name__)


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

        self._aurora_writer_endpoint: Optional[Tuple[str, int]] = None
        self._aurora_reader_endpoint: Optional[Tuple[str, int]] = None

    def __repr__(self) -> str:
        return "\n".join(
            [
                "[[Directory]]",
                "[Aurora Writer]",
                repr(self._aurora_writer),
                "",
                "[Aurora Readers]",
                *map(repr, self._aurora_readers),
                "",
                "[Redshift]",
                repr(self._redshift_cluster),
            ]
        )

    def __getstate__(self) -> Dict[Any, Any]:
        return {
            "config": self._config,
            "aurora_writer": self._aurora_writer,
            "aurora_readers": self._aurora_readers,
            "redshift_cluster": self._redshift_cluster,
            "aurora_writer_endpoint": self._aurora_writer_endpoint,
            "aurora_reader_endpoint": self._aurora_reader_endpoint,
        }

    def __setstate__(self, d: Dict[Any, Any]) -> None:
        self._config = d["config"]
        self._aurora_writer = d["aurora_writer"]
        self._aurora_readers = d["aurora_readers"]
        self._redshift_cluster = d["redshift_cluster"]
        self._aurora_writer_endpoint = d["aurora_writer_endpoint"]
        self._aurora_reader_endpoint = d["aurora_reader_endpoint"]

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

    def aurora_writer(self) -> "AuroraInstanceMetadata":
        assert self._aurora_writer is not None
        return self._aurora_writer

    def aurora_readers(self) -> List["AuroraInstanceMetadata"]:
        return self._aurora_readers

    def redshift_cluster(self) -> "RedshiftClusterMetadata":
        assert self._redshift_cluster is not None
        return self._redshift_cluster

    def aurora_writer_endpoint(self) -> Tuple[str, int]:
        assert self._aurora_writer_endpoint is not None
        return self._aurora_writer_endpoint

    def aurora_reader_endpoint(self) -> Tuple[str, int]:
        assert self._aurora_reader_endpoint is not None
        return self._aurora_reader_endpoint

    async def refresh(self) -> None:
        (
            aurora_writer,
            aurora_readers,
            writer_endpoint,
            reader_endpoint,
        ) = await self._refresh_aurora()
        redshift = await self._refresh_redshift()

        self._aurora_writer = aurora_writer
        self._aurora_readers.clear()
        self._aurora_readers.extend(aurora_readers)
        self._redshift_cluster = redshift

        self._aurora_writer_endpoint = writer_endpoint
        self._aurora_reader_endpoint = reader_endpoint

    async def _refresh_aurora(
        self,
    ) -> Tuple[
        "AuroraInstanceMetadata",
        List["AuroraInstanceMetadata"],
        Tuple[str, int],
        Tuple[str, int],
    ]:
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(None, self._call_describe_aurora_cluster)

        new_writer: Optional[AuroraInstanceMetadata] = None
        new_readers: List[AuroraInstanceMetadata] = []
        cluster_info = response["DBClusters"][0]

        for instance_info in cluster_info["DBClusterMembers"]:
            instance_id = instance_info["DBInstanceIdentifier"]
            is_writer = instance_info["IsClusterWriter"]

            if is_writer:
                assert new_writer is None
                new_writer = await self._refresh_aurora_instance(instance_id)
            else:
                if "-replica-" not in instance_id:
                    logger.debug(
                        "Ignoring Aurora instance %s because it is not named as a replica.",
                        instance_id,
                    )
                    continue

                reader_info = await self._refresh_aurora_instance(instance_id)
                reader_status = reader_info.status()
                if (
                    reader_status == RdsStatus.Deleting
                    or reader_status == RdsStatus.DeletePrecheck
                    or reader_status == RdsStatus.Failed
                ):
                    logger.debug(
                        "Ignoring Aurora instance %s because it has an invalid status %s",
                        instance_id,
                        str(reader_status),
                    )
                    continue
                new_readers.append(reader_info)

        assert new_writer is not None
        new_readers.sort()

        writer_endpoint = cluster_info["Endpoint"]
        reader_endpoint = cluster_info["ReaderEndpoint"]
        port = int(cluster_info["Port"])

        return (
            new_writer,
            new_readers,
            (writer_endpoint, port),
            (reader_endpoint, port),
        )

    async def _refresh_aurora_instance(
        self, instance_id: str
    ) -> "AuroraInstanceMetadata":
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None, self._call_describe_aurora_instance, instance_id
        )
        instance_data = response["DBInstances"][0]
        kwargs = {
            "instance_id": instance_id,
            "resource_id": instance_data["DbiResourceId"],
            "endpoint_address": instance_data["Endpoint"]["Address"],
            "endpoint_port": instance_data["Endpoint"]["Port"],
            "status": RdsStatus.from_str(instance_data["DBInstanceStatus"]),
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
            "instance_type": cluster["NodeType"],
            "num_nodes": cluster["NumberOfNodes"],
            "availability_status": RedshiftAvailabilityStatus.from_str(
                cluster["ClusterAvailabilityStatus"]
            ),
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
        status: RdsStatus,
    ) -> None:
        self._instance_id = instance_id
        self._resource_id = resource_id
        self._endpoint_address = endpoint_address
        self._endpoint_port = endpoint_port
        self._status = status

    def __repr__(self) -> str:
        return "\n".join(
            [
                "AuroraInstanceMetadata",
                "  Instance ID: {}".format(self._instance_id),
                "  Resource ID: {}".format(self._resource_id),
                "  Endpoint: {}:{}".format(
                    self._endpoint_address, str(self._endpoint_port)
                ),
                "  Status: {}".format(self._status),
            ]
        )

    def instance_id(self) -> str:
        return self._instance_id

    def resource_id(self) -> str:
        return self._resource_id

    def endpoint(self) -> Tuple[str, int]:
        return (self._endpoint_address, self._endpoint_port)

    def status(self) -> RdsStatus:
        return self._status

    def is_replica(self) -> bool:
        return "-replica-" in self._instance_id

    def version_and_offset(self) -> Tuple[int, int]:
        """
        Returns the blueprint version that was responsible for first creating
        this instance. Also includes the instance offset. This offset is always
        0 for the primary instance.
        """
        # This assumes that instances are named using our special format.
        #   <cluster_id>-<role>-<index>-<bp version>
        # Note that `<index>` is missing from primary instances.
        parts = self._instance_id.split("-")
        role = parts[1]
        bp_version = int(parts[-1])
        if role == "primary":
            offset = 0
        else:
            offset = int(parts[-2])
        return bp_version, offset

    def __lt__(self, other: "AuroraInstanceMetadata") -> bool:
        return self.instance_id() < other.instance_id()


class RedshiftClusterMetadata:
    def __init__(
        self,
        endpoint_address: str,
        endpoint_port: int,
        instance_type: str,
        num_nodes: int,
        availability_status: RedshiftAvailabilityStatus,
    ) -> None:
        self._endpoint_address = endpoint_address
        self._endpoint_port = endpoint_port
        self._instance_type = instance_type
        self._num_nodes = num_nodes
        self._availability_status = availability_status

    def __repr__(self) -> str:
        return "\n".join(
            [
                "RedshiftClusterMetadata",
                "  Endpoint: {}:{}".format(
                    self._endpoint_address, str(self._endpoint_port)
                ),
                "  Instance Type: {}".format(self._instance_type),
                "  Num. of Nodes: {}".format(self._num_nodes),
                "  Status: {}".format(self._availability_status),
            ]
        )

    def endpoint(self) -> Tuple[str, int]:
        return (self._endpoint_address, self._endpoint_port)

    def instance_type(self) -> str:
        return self._instance_type

    def num_nodes(self) -> int:
        return self._num_nodes

    def availability_status(self) -> RedshiftAvailabilityStatus:
        return self._availability_status
