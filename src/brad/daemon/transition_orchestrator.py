import asyncio
import logging
from typing import Optional

from brad.blueprint import Blueprint
from brad.blueprint.diff.blueprint import BlueprintDiff
from brad.blueprint.diff.provisioning import ProvisioningDiff
from brad.blueprint.manager import BlueprintManager
from brad.blueprint.provisioning import Provisioning
from brad.config.file import ConfigFile
from brad.provisioning.rds import RdsProvisioningManager
from brad.provisioning.redshift import RedshiftProvisioningManager

logger = logging.getLogger(__name__)


class TransitionOrchestrator:
    def __init__(
        self,
        config: ConfigFile,
        blueprint_mgr: BlueprintManager,
        next_blueprint: Blueprint,
    ) -> None:
        self._config = config
        self._blueprint_mgr = blueprint_mgr
        self._next_blueprint = next_blueprint
        self._rds = RdsProvisioningManager(config)
        self._redshift = RedshiftProvisioningManager(config)

    async def run_pre_transition(self) -> None:
        curr_blueprint = self._blueprint_mgr.get_blueprint()
        diff = BlueprintDiff.of(curr_blueprint, self._next_blueprint)
        if diff is None:
            # Nothing to do.
            return

        next_version = await self._blueprint_mgr.start_transition(self._next_blueprint)

        aurora_diff = diff.aurora_diff()
        redshift_diff = diff.redshift_diff()

        aurora_awaitable = self._run_aurora_pre_transition(
            curr_blueprint.aurora_provisioning(),
            self._next_blueprint.aurora_provisioning(),
            aurora_diff,
            next_version,
        )
        redshift_awaitable = self._run_redshift_pre_transition(redshift_diff)
        await asyncio.gather(aurora_awaitable, redshift_awaitable)

    async def run_post_transition(self) -> None:
        pass

    def _requires_aurora_failover(
        self, aurora_diff: Optional[ProvisioningDiff]
    ) -> bool:
        if aurora_diff is None:
            return False
        return aurora_diff.new_instance_type() is not None

    async def _run_aurora_pre_transition(
        self,
        old: Provisioning,
        new: Provisioning,
        diff: Optional[ProvisioningDiff],
        next_version: int,
    ) -> None:
        if diff is None:
            return

        if new.num_nodes() == 0:
            # We pause the cluster in the post-transition step.
            return

        if old.num_nodes() == 0:
            await self._rds.start_cluster(
                self._config.aurora_cluster_id, wait_until_available=True
            )
            await self._blueprint_mgr.refresh_directory()

        # NOTE: We will need a more robust process to deal with cases where we
        # are at the replica limit (max. 15 replicas).

        if old.instance_type() != new.instance_type():
            # Handle the primary first.
            new_primary_instance = _AURORA_PRIMARY_FORMAT.format(
                cluster_id=self._config.aurora_cluster_id,
                version=str(next_version),
            )
            logger.debug("Creating new Aurora replica: %s", new_primary_instance)
            await self._rds.create_replica(
                self._config.aurora_cluster_id,
                new_primary_instance,
                new,
                wait_until_available=True,
            )
            logger.debug(
                "Failing over %s to the new replica: %s",
                self._config.aurora_cluster_id,
                new_primary_instance,
            )
            await self._rds.run_primary_failover(
                self._config.aurora_cluster_id, new_primary_instance
            )
            logger.debug("Failover complete for %s", self._config.aurora_cluster_id)
            await self._blueprint_mgr.refresh_directory()

            replicas_to_modify = min(new.num_nodes() - 1, old.num_nodes() - 1)

            # Modify replicas one-by-one. Note that this logic causes the reader
            # to go down if there is only one read replica.
            for idx, replica in enumerate(
                self._blueprint_mgr.get_directory().aurora_readers()
            ):
                if idx >= replicas_to_modify:
                    break

                logger.debug(
                    "Changing instance %s to %s",
                    replica.instance_id(),
                    new.instance_type(),
                )
                await self._rds.change_instance_type(
                    replica.instance_id(), new, wait_until_available=True
                )

        new_replica_count = new.num_nodes() - 1
        old_replica_count = old.num_nodes() - 1
        if (
            new_replica_count > 0
            and old_replica_count > 0
            and new_replica_count > old_replica_count
        ):
            next_index = old_replica_count
            while next_index < new_replica_count:
                new_replica_id = _AURORA_REPLICA_FORMAT.format(
                    cluster_id=self._config.aurora_cluster_id,
                    version=next_version,
                    index=next_index,
                )
                logger.debug("Creating replica %s", new_replica_id)
                await self._rds.create_replica(
                    self._config.aurora_cluster_id,
                    new_replica_id,
                    new,
                    wait_until_available=True,
                )
                next_index += 1
            await self._blueprint_mgr.refresh_directory()

        # Aurora's pre-transition work is complete!

    async def _run_redshift_pre_transition(
        self, diff: Optional[ProvisioningDiff]
    ) -> None:
        if diff is None:
            return


# Note that `version` is the blueprint version when the instance was created. It
# may not represent the current blueprint version.
_AURORA_PRIMARY_FORMAT = "{cluster_id}-primary-{version}"
_AURORA_REPLICA_FORMAT = "{cluster_id}-replica-{index}-{version}"
