import asyncio
import logging
from typing import Optional, Callable

from brad.blueprint.diff.blueprint import BlueprintDiff
from brad.blueprint.diff.provisioning import ProvisioningDiff
from brad.blueprint.diff.table import TableDiff
from brad.blueprint.manager import BlueprintManager
from brad.blueprint.provisioning import Provisioning
from brad.blueprint.sql_gen.table import TableSqlGenerator
from brad.blueprint.state import TransitionState
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.data_sync.execution.context import ExecutionContext
from brad.data_sync.operators.drop_tables import DropTables
from brad.data_sync.operators.load_from_s3 import LoadFromS3
from brad.data_sync.operators.unload_to_s3 import UnloadToS3
from brad.front_end.engine_connections import EngineConnections
from brad.provisioning.directory import Directory
from brad.provisioning.rds import RdsProvisioningManager
from brad.provisioning.redshift import RedshiftProvisioningManager

logger = logging.getLogger(__name__)


class TransitionOrchestrator:
    def __init__(
        self,
        config: ConfigFile,
        blueprint_mgr: BlueprintManager,
    ) -> None:
        self._config = config
        self._blueprint_mgr = blueprint_mgr
        self._rds = RdsProvisioningManager(config)
        self._redshift = RedshiftProvisioningManager(config)
        self._waiting_for_front_ends = 0

        self._refresh_transition_metadata()

    def next_version(self) -> Optional[int]:
        """
        Returns the version this class is transitioning to. If `None`, it
        indicates no transition is in progress.
        """
        return self._next_version

    async def run_prepare_then_transition(
        self, on_instance_identity_change: Optional[Callable[[], None]] = None
    ) -> None:
        """
        Prepares the provisioning for a graceful transition, and then executes
        the transition. After this method returns, the next blueprint will be
        active. Clients (i.e., front ends) should switch to the next blueprint.
        After all clients have switched, run `run_post_transition()` to clean up
        any old provisionings.
        """

        if self._tm.state != TransitionState.Transitioning:
            return

        if self._diff is None:
            # Nothing to do.
            await self._blueprint_mgr.update_transition_state(
                TransitionState.TransitionedPreCleanUp
            )
            self._refresh_transition_metadata()
            return

        assert self._curr_blueprint is not None
        assert self._next_blueprint is not None
        assert self._next_version is not None

        # 1. Re-provision Aurora and Redshift as needed
        aurora_diff = self._diff.aurora_diff()
        redshift_diff = self._diff.redshift_diff()

        aurora_awaitable = self._run_aurora_pre_transition(
            self._curr_blueprint.aurora_provisioning(),
            self._next_blueprint.aurora_provisioning(),
            aurora_diff,
            self._next_version,
            on_instance_identity_change,
        )
        redshift_awaitable = self._run_redshift_pre_transition(
            self._curr_blueprint.redshift_provisioning(),
            self._next_blueprint.redshift_provisioning(),
            redshift_diff,
        )
        await asyncio.gather(aurora_awaitable, redshift_awaitable)
        logger.debug("Aurora and Redshift provisioning changes complete.")

        # 2. Sync tables (TODO: discuss more efficient alternatives -
        # possibly add a filter of tables to run_sync)
        ran_sync = await self._data_sync_executor.run_sync(
            self._blueprint_mgr.get_blueprint()
        )
        logger.debug(
            f"""Completed data sync step during transition. 
            There were {'some' if ran_sync else 'no'} new writes to sync"""
        )

        # 3. Create tables in new locations as needed
        directory = Directory(self._config)
        asyncio.run(directory.refresh())

        cxns = EngineConnections.connect_sync(
            self._config,
            directory,
            schema_name=self._curr_blueprint.schema_name(),
            autocommit=False,
            specific_engines=None,
        )

        sql_gen = TableSqlGenerator(self._config, self._next_blueprint)

        for diff in self._diff.table_diffs():
            for location in diff.added_locations:
                logger.info(
                    "Creating table '%s' on %s...",
                    diff.table_name,
                    location,
                )
                queries, db_type = sql_gen.generate_create_table_sql(
                    diff.table_name, location
                )
                conn = cxns.get_connection(db_type)
                cursor = conn.cursor_sync()
                for q in queries:
                    logger.debug("Running on %s: %s", str(db_type), q)
                    cursor.execute_sync(q)

        # 4. Load tables into new locations
        table_awaitables = []
        for diff in self._diff.table_diffs():
            if diff.added_locations is not None:
                table_awaitables.append(self._enforce_table_diff_additions(diff))
        await asyncio.gather(table_awaitables)

        logger.debug("Table movement complete.")

        logger.debug("Pre-transition steps complete.")

        await self._blueprint_mgr.update_transition_state(
            TransitionState.TransitionedPreCleanUp
        )
        self._refresh_transition_metadata()

    def set_waiting_for_front_ends(self, value: int) -> None:
        self._waiting_for_front_ends = value

    def waiting_for_front_ends(self) -> int:
        return self._waiting_for_front_ends

    def decrement_waiting_for_front_ends(self) -> None:
        self._waiting_for_front_ends -= 1

    async def run_clean_up_after_transition(self) -> None:
        if (
            self._tm.state != TransitionState.TransitionedPreCleanUp
            and self._tm.state != TransitionState.CleaningUp
        ):
            return

        if self._tm.state == TransitionState.TransitionedPreCleanUp:
            await self._blueprint_mgr.update_transition_state(
                TransitionState.CleaningUp
            )
            self._refresh_transition_metadata()

        if self._diff is None:
            await self._blueprint_mgr.update_transition_state(TransitionState.Stable)
            return

        assert self._curr_blueprint is not None
        assert self._next_blueprint is not None

        aurora_awaitable = self._run_aurora_post_transition(
            self._curr_blueprint.aurora_provisioning(),
            self._next_blueprint.aurora_provisioning(),
            self._diff.aurora_diff(),
            self._diff.table_diffs(),
        )
        redshift_awaitable = self._run_redshift_post_transition(
            self._diff.redshift_diff(), self._diff.table_diffs()
        )
        athena_awaitable = self._run_athena_post_transition(self._diff.table_diffs())
        await asyncio.gather(aurora_awaitable, redshift_awaitable, athena_awaitable)
        logger.debug("Post-transition steps complete.")

        await self._blueprint_mgr.update_transition_state(TransitionState.Stable)

    def _refresh_transition_metadata(self) -> None:
        self._tm = self._blueprint_mgr.get_transition_metadata()
        if self._tm.state == TransitionState.Stable:
            self._diff = None
            self._curr_blueprint = None
            self._next_blueprint = None
            self._next_version = None
            return

        assert self._tm.next_blueprint is not None
        assert self._tm.next_version is not None
        self._diff = BlueprintDiff.of(self._tm.curr_blueprint, self._tm.next_blueprint)
        self._curr_blueprint = self._tm.curr_blueprint
        self._next_blueprint = self._tm.next_blueprint
        self._next_version = self._tm.next_version

    async def _run_aurora_pre_transition(
        self,
        old: Provisioning,
        new: Provisioning,
        diff: Optional[ProvisioningDiff],
        next_version: int,
        on_instance_identity_change: Optional[Callable[[], None]],
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

            # When "shutting down" a cluster, we pause it instead of deleting
            # instances. So on startup, the actual number of replicas will not
            # match what is passed in.
            directory = self._blueprint_mgr.get_directory()
            paused_aurora_nodes = 1 + len(directory.aurora_readers())
            old = Provisioning(old.instance_type(), paused_aurora_nodes)

        # NOTE: We will need a more robust process to deal with cases where we
        # are at the replica limit (max. 15 replicas).

        if old.instance_type() != new.instance_type():
            # Handle the primary first.
            old_primary_instance = (
                self._blueprint_mgr.get_directory().aurora_writer().instance_id()
            )
            new_primary_instance = _AURORA_PRIMARY_FORMAT.format(
                cluster_id=self._config.aurora_cluster_id,
                version=str(next_version).zfill(5),
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
                self._config.aurora_cluster_id,
                new_primary_instance,
                wait_until_complete=True,
            )
            logger.debug("Failover complete for %s", self._config.aurora_cluster_id)

            logger.debug("Deleting the old primary: %s", old_primary_instance)
            await self._rds.delete_replica(old_primary_instance)
            logger.debug("Done deleting the old primary: %s", old_primary_instance)

            await self._blueprint_mgr.refresh_directory()
            if on_instance_identity_change is not None:
                # The primary changed. We run the callback so that clients can
                # update any cached state that relies on instance identities
                # (e.g., Performance Insights metrics).
                on_instance_identity_change()

            replicas_to_modify = min(new.num_nodes() - 1, old.num_nodes() - 1)

            if replicas_to_modify == 1 and old.num_nodes() - 1 == 1:
                # Special case: The current blueprint only has one read replica
                # and we need to modify it to transition to the next blueprint.
                existing_readers = self._blueprint_mgr.get_directory().aurora_readers()
                assert len(existing_readers) == 1
                existing_replica_id = existing_readers[0].instance_id()

                new_replica_id = _AURORA_REPLICA_FORMAT.format(
                    cluster_id=self._config.aurora_cluster_id,
                    version=str(next_version).zfill(5),
                    index=str(0).zfill(2),
                )
                logger.debug("Creating replica %s", new_replica_id)
                await self._rds.create_replica(
                    self._config.aurora_cluster_id,
                    new_replica_id,
                    new,
                    wait_until_available=True,
                )

                logger.debug("Deleting the old replica: %s", existing_replica_id)
                await self._rds.delete_replica(existing_replica_id)
                await self._blueprint_mgr.refresh_directory()
                if on_instance_identity_change is not None:
                    on_instance_identity_change()

            else:
                # Modify replicas one-by-one. At most one reader replica is down
                # at any time - but we consider this acceptable.
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

        new_replica_count = max(new.num_nodes() - 1, 0)
        old_replica_count = max(old.num_nodes() - 1, 0)
        if new_replica_count > 0 and new_replica_count > old_replica_count:
            next_index = old_replica_count
            while next_index < new_replica_count:
                new_replica_id = _AURORA_REPLICA_FORMAT.format(
                    cluster_id=self._config.aurora_cluster_id,
                    version=str(next_version).zfill(5),
                    index=str(next_index).zfill(2),
                )
                logger.debug("Creating replica %s", new_replica_id)
                # Ideally we wait for the replicas to finish creation in
                # parallel. Because of how we make the boto3 client async,
                # there's a possibility of having multiple API calls in flight
                # at the same time, which boto3 does not support. To be safe, we
                # just run these replica creations sequentially.
                await self._rds.create_replica(
                    self._config.aurora_cluster_id,
                    new_replica_id,
                    new,
                    wait_until_available=True,
                )
                next_index += 1
            await self._blueprint_mgr.refresh_directory()

        # Aurora's pre-transition work is complete!

    async def _run_aurora_post_transition(
        self,
        old: Provisioning,
        new: Provisioning,
        diff: Optional[ProvisioningDiff],
        table_diffs: Optional[list[TableDiff]],
    ) -> None:
        if diff is None:
            # Nothing to do.
            return

        if new.num_nodes() == 0:
            # Special case (shutting down the cluster).
            await self._rds.pause_cluster(self._config.aurora_cluster_id)
            return

        old_replica_count = max(old.num_nodes() - 1, 0)
        new_replica_count = max(new.num_nodes() - 1, 0)

        if old_replica_count > 0 and new_replica_count < old_replica_count:
            await self._blueprint_mgr.refresh_directory()
            replicas = self._blueprint_mgr.get_directory().aurora_readers()
            delete_index = old_replica_count - 1
            while delete_index >= new_replica_count:
                logger.debug(
                    "Deleting Aurora replica %s...",
                    replicas[delete_index].instance_id(),
                )
                await self._rds.delete_replica(replicas[delete_index].instance_id())
                delete_index -= 1

        # Drop removed tables
        if table_diffs is None:
            # Nothing to do.
            return

        to_drop = []
        for table_diff in table_diffs:
            if Engine.Aurora in table_diff.removed_locations():
                to_drop.append(table_diff.table_name())
        d = DropTables(to_drop, Engine.Aurora)
        ctx = self._new_execution_context(Engine.Aurora)
        d.execute(ctx)

        # Aurora's post-transition work is complete!

    async def _run_redshift_pre_transition(
        self,
        old: Provisioning,
        new: Provisioning,
        diff: Optional[ProvisioningDiff],
    ) -> None:
        if diff is None:
            # Nothing to do.
            return

        # Handle special cases: Starting up from 0 or going to 0.
        if diff.new_num_nodes() == 0:
            # This is handled post-transition.
            return

        if old.num_nodes() == 0:
            logger.debug(
                "Resuming Redshift cluster %s", self._config.redshift_cluster_id
            )
            existing = await self._redshift.resume_and_fetch_existing_provisioning(
                self._config.redshift_cluster_id
            )
            if existing == new:
                return

            # We shut down clusters by pausing them. On restart, they resume
            # with their old provisioning.
            old = existing

        # Resizes are OK because Redshift maintains read-availability during the
        # resize.
        is_classic = self._redshift.must_use_classic_resize(old, new)
        if is_classic:
            logger.debug(
                "Running Redshift classic resize. Old: %s, New: %s", str(old), str(new)
            )
            await self._redshift.classic_resize(
                self._config.redshift_cluster_id, new, wait_until_available=True
            )
        else:
            logger.debug(
                "Running Redshift elastic resize. Old: %s, New: %s", str(old), str(new)
            )
            await self._redshift.elastic_resize(
                self._config.redshift_cluster_id, new, wait_until_available=True
            )

        # Redshift's pre-transition work is complete!

    async def _run_redshift_post_transition(
        self, diff: Optional[ProvisioningDiff], table_diffs: Optional[list[TableDiff]]
    ) -> None:
        if diff is None:
            # Nothing to do.
            return

        if diff.new_num_nodes() == 0:
            logger.debug(
                "Pausing Redshift cluster %s", self._config.redshift_cluster_id
            )
            await self._redshift.pause_cluster(self._config.redshift_cluster_id)

        # Drop removed tables
        if table_diffs is None:
            # Nothing to do.
            return

        to_drop = []
        for table_diff in table_diffs:
            if Engine.Redshift in table_diff.removed_locations():
                to_drop.append(table_diff.table_name())
        d = DropTables(to_drop, Engine.Redshift)
        ctx = self._new_execution_context(Engine.Redshift)
        d.execute(ctx)

    async def _run_athena_post_transition(
        self,
        table_diffs: Optional[list[TableDiff]],
    ) -> None:
        if table_diffs is None:
            # Nothing to do.
            return

        # Drop removed tables
        to_drop = []
        for table_diff in self._diff.table_diffs():
            if Engine.Athena in table_diff.removed_locations():
                to_drop.append(table_diff.table_name())
        d = DropTables(to_drop, Engine.Athena)
        ctx = self._new_execution_context(Engine.Athena)
        d.execute(ctx)

    async def _enforce_table_diff_additions(self, diff: TableDiff) -> None:
        await self._unload_table_if_needed(diff.table)

        table_loading_awaitables = []
        for e in diff.added_locations:
            table_loading_awaitables.append(
                self._load_table_to_engine(diff.table_name, e)
            )

    async def _unload_table_if_needed(self, table_name: str) -> None:
        curr_locations = self._curr_blueprint.get_table_locations(table_name)
        temp_s3_path = (
            f"transition/{table_name}.tbl"  # FIXME: different path convention?
        )

        # The logic here assumes that all engines have the most recent version.

        # If the table already exists in Athena, no reason to write it out again.
        if Engine.Athena in curr_locations:
            logger.debug(
                f"""In transition: table {table_name} exists on S3, 
                         no need to write out temporary table"""
            )
        elif Engine.Redshift in curr_locations:  # Faster to write out from Redshift
            u = UnloadToS3(table_name, temp_s3_path, engine=Engine.Redshift)
            ctx = self._new_execution_context(Engine.Redshift)
            u.execute(ctx)
            logger.debug(
                f"In transition: table {table_name} written to S3 from Redshift."
            )
        elif Engine.Aurora in curr_locations:
            u = UnloadToS3(table_name, temp_s3_path, engine=Engine.Aurora)
            ctx = self._new_execution_context(Engine.Aurora)
            u.execute(ctx)
            logger.debug(
                f"In transition: table {table_name} written to S3 from Aurora."
            )
        else:
            logger.error(
                f"""In transition: table {table_name} does not exist 
                         on any engine in current blueprint."""
            )

    async def _load_table_to_engine(self, table_name: str, e: Engine) -> None:
        temp_s3_path = f"transition/{table_name}.tbl"
        ctx = self._new_execution_context(e)

        l = LoadFromS3(table_name, temp_s3_path, e)
        l.execute(ctx)
        logger.debug(f"In transition: table {table_name} loaded to {e}.")

    def _new_execution_context(self, e: Engine) -> ExecutionContext:
        assert self._engines is not None
        return ExecutionContext(
            aurora=(
                self._engines.get_connection(Engine.Aurora)
                if e == Engine.Aurora
                else None
            ),
            athena=(
                self._engines.get_connection(Engine.Athena)
                if e == Engine.Athena
                else None
            ),
            redshift=(
                self._engines.get_connection(Engine.Redshift)
                if e == Engine.Redshift
                else None
            ),
            blueprint=self._blueprint_mgr.get_blueprint(),
            config=self._config,
        )


# Note that `version` is the blueprint version when the instance was created. It
# may not represent the current blueprint version.
_AURORA_PRIMARY_FORMAT = "{cluster_id}-primary-{version}"
_AURORA_REPLICA_FORMAT = "{cluster_id}-replica-{index}-{version}"
