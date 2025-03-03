import asyncio
import logging
from typing import Optional, Callable

from brad.blueprint.diff.blueprint import BlueprintDiff
from brad.blueprint.diff.provisioning import ProvisioningDiff
from brad.blueprint.diff.table import TableDiff
from brad.blueprint.manager import BlueprintManager
from brad.blueprint.provisioning import Provisioning
from brad.blueprint.sql_gen.table import TableSqlGenerator, comma_separated_column_names
from brad.blueprint.state import TransitionState
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.config.strings import (
    source_table_name,
    shadow_table_name,
    AURORA_SEQ_COLUMN,
    AURORA_EXTRACT_PROGRESS_TABLE_NAME,
)
from brad.config.system_event import SystemEvent
from brad.daemon.system_event_logger import SystemEventLogger
from brad.data_sync.execution.context import ExecutionContext
from brad.data_sync.operators.drop_aurora_triggers import DropAuroraTriggers
from brad.data_sync.execution.executor import DataSyncExecutor
from brad.data_sync.operators.delete_s3_objects import DeleteS3Objects
from brad.data_sync.operators.drop_tables import DropTables
from brad.data_sync.operators.drop_views import DropViews
from brad.data_sync.operators.load_from_s3 import LoadFromS3
from brad.data_sync.operators.unload_to_s3 import UnloadToS3
from brad.front_end.engine_connections import EngineConnections
from brad.provisioning.rds import RdsProvisioningManager
from brad.provisioning.redshift import RedshiftProvisioningManager
from brad.utils.assertions import nonsilent_assert

logger = logging.getLogger(__name__)


class TransitionOrchestrator:
    def __init__(
        self,
        config: ConfigFile,
        blueprint_mgr: BlueprintManager,
        system_event_logger: Optional[SystemEventLogger] = None,
    ) -> None:
        self._config = config
        self._blueprint_mgr = blueprint_mgr
        self._rds = RdsProvisioningManager(config)
        self._redshift = RedshiftProvisioningManager(config)
        self._waiting_for_front_ends = 0
        self._data_sync_executor = DataSyncExecutor(self._config, self._blueprint_mgr)
        self._cxns: Optional[EngineConnections] = None
        self._system_event_logger = system_event_logger

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

        nonsilent_assert(self._curr_blueprint is not None)
        nonsilent_assert(self._next_blueprint is not None)
        nonsilent_assert(self._next_version is not None)
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

        if self._config.disable_table_movement:
            logger.debug("Table movement during transitions is disabled.")

            # Note that we only use preset Redshift clusters when table movement
            # is disabled.
            if self._config.use_preset_redshift_clusters and redshift_diff is not None:
                preset_cluster_id = self._config.get_preset_redshift_cluster_id(
                    self._next_blueprint.redshift_provisioning()
                )
                # Here we switch the directory over to/from the preset cluster.
                self._blueprint_mgr.get_directory().set_override_redshift_cluster_id(
                    preset_cluster_id
                )
                await self._blueprint_mgr.refresh_directory()
                if on_instance_identity_change is not None:
                    # The Redshift cluster may have changed.
                    on_instance_identity_change()

            await self._run_prepare_then_transition_cleanup()
            return

        # 2. Sync tables (TODO: discuss more efficient alternatives -
        # possibly add a filter of tables to run_sync)
        if not self._config.skip_sync_before_movement:
            await self._data_sync_executor.establish_connections()
            ran_sync = await self._data_sync_executor.run_sync(
                self._blueprint_mgr.get_blueprint()
            )
            logger.debug(
                """Completed data sync step during transition. """
                f"""There were {'some' if ran_sync else 'no'} new writes to sync"""
            )
        else:
            logger.info("Not running table sync before movement.")

        if self._system_event_logger is not None:
            self._system_event_logger.log(SystemEvent.PreTableMovementStarted)

        # 3. Create tables in new locations as needed
        directory = self._blueprint_mgr.get_directory()

        active_engines = {Engine.Aurora, Engine.Redshift, Engine.Athena}
        if self._curr_blueprint.aurora_provisioning().num_nodes() == 0:
            active_engines.remove(Engine.Aurora)
        if self._curr_blueprint.redshift_provisioning().num_nodes() == 0:
            active_engines.remove(Engine.Redshift)

        self._cxns = EngineConnections.connect_sync(
            self._config,
            directory,
            schema_name=self._curr_blueprint.schema_name(),
            autocommit=False,
            specific_engines=active_engines,
        )

        sql_gen = TableSqlGenerator(self._config, self._next_blueprint)

        for diff in self._diff.table_diffs():
            table = self._curr_blueprint.get_table(diff.table_name())
            for location in diff.added_locations():
                logger.info(
                    "Creating table '%s' on %s...",
                    table.name,
                    location,
                )
                queries, db_type = sql_gen.generate_create_table_sql(table, location)
                conn = self._cxns.get_connection(db_type)
                cursor = conn.cursor_sync()
                for q in queries:
                    logger.debug("Running on %s: %s", str(db_type), q)
                    cursor.execute_sync(q)

                if location == Engine.Aurora:
                    self._cxns.get_connection(Engine.Aurora).cursor_sync().commit_sync()
                elif location == Engine.Redshift:
                    self._cxns.get_connection(
                        Engine.Redshift
                    ).cursor_sync().commit_sync()

        # 4. Load tables into new locations
        table_awaitables = []
        for diff in self._diff.table_diffs():
            if len(diff.added_locations()) > 0:
                table_awaitables.append(self._enforce_table_diff_additions(diff))
        await asyncio.gather(*table_awaitables)

        logger.info("Table movement complete.")
        if self._system_event_logger is not None:
            self._system_event_logger.log(SystemEvent.PreTableMovementCompleted)

        # Close connections
        await self._cxns.close()
        self._cxns = None

        await self._run_prepare_then_transition_cleanup()

    async def _run_prepare_then_transition_cleanup(self) -> None:
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

        nonsilent_assert(self._curr_blueprint is not None)
        nonsilent_assert(self._next_blueprint is not None)
        assert self._curr_blueprint is not None
        assert self._next_blueprint is not None

        if not self._config.disable_table_movement:
            directory = self._blueprint_mgr.get_directory()
            active_engines = {Engine.Aurora, Engine.Redshift, Engine.Athena}
            if self._curr_blueprint.aurora_provisioning().num_nodes() == 0:
                active_engines.remove(Engine.Aurora)
            if self._curr_blueprint.redshift_provisioning().num_nodes() == 0:
                active_engines.remove(Engine.Redshift)

            self._cxns = EngineConnections.connect_sync(
                self._config,
                directory,
                schema_name=self._curr_blueprint.schema_name(),
                autocommit=False,
                specific_engines=active_engines,
            )

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

        # Close connections
        if self._cxns is not None:
            await self._cxns.close()
            self._cxns = None

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

        nonsilent_assert(self._tm.next_blueprint is not None)
        nonsilent_assert(self._tm.next_version is not None)
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
        #
        # We should also take into account the kind of transition to make the
        # change more graceful (e.g., when switching to lower resourced
        # provisionings).

        # Create new replicas first.
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

        if old.instance_type() != new.instance_type():
            # Handle the replicas first (need to change their instance type).
            replicas_to_modify = min(new.num_nodes() - 1, old.num_nodes() - 1)
            if replicas_to_modify == 1 and old.num_nodes() - 1 == 1:
                # Special case: The current blueprint only has one read replica
                # and we need to modify it to transition to the next blueprint.
                existing_readers = self._blueprint_mgr.get_directory().aurora_readers()

                # Find the first replica.
                replica_to_replace = None
                for reader_metadata in existing_readers:
                    created_version, offset = reader_metadata.version_and_offset()
                    if offset == 0:
                        nonsilent_assert(created_version != next_version)
                        replica_to_replace = reader_metadata
                        break
                nonsilent_assert(replica_to_replace is not None)
                assert replica_to_replace is not None

                existing_replica_id = replica_to_replace.instance_id()
                logger.debug(
                    "Will create a new replica to replace %s", existing_replica_id
                )

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
                # at any time, but we consider this acceptable.
                for replica in self._blueprint_mgr.get_directory().aurora_readers():
                    created_version, offset = replica.version_and_offset()
                    if created_version == next_version:
                        logger.debug(
                            "Not modifying %s because it is a new replica.",
                            replica.instance_id(),
                        )
                        continue
                    if offset >= replicas_to_modify:
                        logger.debug(
                            "Not modifying %s because it is beyond the replicas we need to modify.",
                            replica.instance_id(),
                        )
                        continue

                    logger.debug(
                        "Changing instance %s to %s",
                        replica.instance_id(),
                        new.instance_type(),
                    )
                    await self._rds.change_instance_type(
                        replica.instance_id(), new, wait_until_available=True
                    )

            # Handle the primary last.
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
            if self._system_event_logger is not None:
                self._system_event_logger.log(SystemEvent.AuroraPrimaryFailover)

            logger.debug("Deleting the old primary: %s", old_primary_instance)
            await self._rds.delete_replica(old_primary_instance)
            logger.debug("Done deleting the old primary: %s", old_primary_instance)

            await self._blueprint_mgr.refresh_directory()
            if on_instance_identity_change is not None:
                # The primary changed. We run the callback so that clients can
                # update any cached state that relies on instance identities
                # (e.g., Performance Insights metrics).
                on_instance_identity_change()

        # Aurora's pre-transition work is complete!

    async def _run_aurora_post_transition(
        self,
        old: Provisioning,
        new: Provisioning,
        diff: Optional[ProvisioningDiff],
        table_diffs: Optional[list[TableDiff]],
    ) -> None:
        # Drop removed tables.
        assert self._curr_blueprint is not None
        aurora_on = self._curr_blueprint.aurora_provisioning().num_nodes() > 0
        if (
            aurora_on
            and table_diffs is not None
            and len(table_diffs) > 0
            and self._config.disable_table_movement is False
            and self._config.skip_aurora_table_deletion is False
        ):
            if self._system_event_logger is not None:
                self._system_event_logger.log(
                    SystemEvent.PostTableMovementStarted, "aurora"
                )

            views_to_drop = []
            triggers_to_drop = []
            tables_to_drop = []
            for table_diff in table_diffs:
                if Engine.Aurora in table_diff.removed_locations():
                    views_to_drop.append(table_diff.table_name())
                    triggers_to_drop.append(table_diff.table_name())
                    tables_to_drop.append(source_table_name(table_diff.table_name()))
                    tables_to_drop.append(shadow_table_name(table_diff.table_name()))

            logger.info("In transition: Dropping Aurora views %s", str(views_to_drop))
            logger.info(
                "In transition: Dropping Aurora triggers %s", str(triggers_to_drop)
            )
            logger.info("In transition: Dropping Aurora tables %s", str(tables_to_drop))

            ctx = self._new_execution_context()

            dv = DropViews(views_to_drop, Engine.Aurora)
            await dv.execute(ctx)

            dat = DropAuroraTriggers(triggers_to_drop)
            await dat.execute(ctx)

            dt = DropTables(tables_to_drop, Engine.Aurora)
            await dt.execute(ctx)

            nonsilent_assert(self._cxns is not None)
            assert self._cxns is not None
            self._cxns.get_connection(Engine.Aurora).cursor_sync().commit_sync()

            if self._system_event_logger is not None:
                self._system_event_logger.log(
                    SystemEvent.PostTableMovementCompleted, "aurora"
                )

        # Change the provisioning.
        if diff is not None:
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

        # Handle special case: Preset clusters.
        if self._config.use_preset_redshift_clusters:
            candidate_cluster = self._config.get_preset_redshift_cluster_id(new)
            if candidate_cluster is not None:
                logger.info(
                    "Will use preset cluster %s with the requested new provisioning %s.",
                    candidate_cluster,
                    new,
                )
                return

        # Handle special case: Going to 0.
        if diff.new_num_nodes() == 0:
            # This is handled post-transition.
            return

        logger.info(
            "Using original Redshift cluster: %s", self._config.redshift_cluster_id
        )
        # We always resume the cluster and fetch its provisioning to make sure
        # we stay in sync with what the actual provisioning is. This is
        # important when we switch from a preset cluster. The resume will be a
        # no-op if the cluster is already running.
        logger.debug("Resuming Redshift cluster %s", self._config.redshift_cluster_id)
        existing = await self._redshift.resume_and_fetch_existing_provisioning(
            self._config.redshift_cluster_id
        )
        if existing == new:
            return

        # We shut down clusters by pausing them. On restart, they resume
        # with their old provisioning. This is also needed if we are switching
        # away from a preset cluster.
        old = existing

        # Resizes are OK because Redshift maintains read-availability during the
        # resize.
        is_classic = self._redshift.must_use_classic_resize(old, new)
        resize_completed = False
        if not is_classic:
            logger.debug(
                "Running Redshift elastic resize. Old: %s, New: %s", str(old), str(new)
            )
            resize_completed = await self._redshift.elastic_resize(
                self._config.redshift_cluster_id, new, wait_until_available=True
            )

        # Sometimes the elastic resize will not be supported even though it
        # should be (according to the docs). This lets us fall back to a classic
        # resize, which should always be supported.
        if is_classic or not resize_completed:
            logger.debug(
                "Running Redshift classic resize. Old: %s, New: %s", str(old), str(new)
            )
            await self._redshift.classic_resize(
                self._config.redshift_cluster_id, new, wait_until_complete=True
            )

        # Redshift's pre-transition work is complete!
        await self._blueprint_mgr.refresh_directory()

    async def _run_redshift_post_transition(
        self, diff: Optional[ProvisioningDiff], table_diffs: Optional[list[TableDiff]]
    ) -> None:
        # Drop removed tables
        assert self._curr_blueprint is not None
        redshift_on = self._curr_blueprint.redshift_provisioning().num_nodes() > 0
        if (
            redshift_on
            and table_diffs is not None
            and self._config.disable_table_movement is False
        ):
            if self._system_event_logger is not None:
                self._system_event_logger.log(
                    SystemEvent.PostTableMovementStarted, "redshift"
                )

            to_drop = []
            for table_diff in table_diffs:
                if Engine.Redshift in table_diff.removed_locations():
                    to_drop.append(table_diff.table_name())
            logger.info("In transition: Dropping Redshift tables %s", str(to_drop))
            d = DropTables(to_drop, Engine.Redshift)
            ctx = self._new_execution_context()
            await d.execute(ctx)
            nonsilent_assert(self._cxns is not None)
            assert self._cxns is not None
            self._cxns.get_connection(Engine.Redshift).cursor_sync().commit_sync()

            if self._system_event_logger is not None:
                self._system_event_logger.log(
                    SystemEvent.PostTableMovementCompleted, "redshift"
                )

        # Pause the cluster if we are transitioning to 0 nodes.
        if diff is not None:
            if diff.new_num_nodes() == 0:
                logger.debug(
                    "Pausing Redshift cluster %s", self._config.redshift_cluster_id
                )
                await self._redshift.pause_cluster(self._config.redshift_cluster_id)

    async def _run_athena_post_transition(
        self,
        table_diffs: Optional[list[TableDiff]],
    ) -> None:
        # Drop removed tables
        to_drop = []
        if (
            table_diffs is not None
            and self._config.disable_table_movement is False
            and self._config.skip_athena_table_deletion is False
        ):
            if self._system_event_logger is not None:
                self._system_event_logger.log(
                    SystemEvent.PostTableMovementStarted, "athena"
                )

            for table_diff in table_diffs:
                if Engine.Athena in table_diff.removed_locations():
                    to_drop.append(table_diff.table_name())
            logger.info("In transition: Dropping Athena tables %s", str(to_drop))
            d = DropTables(to_drop, Engine.Athena)
            ctx = self._new_execution_context()
            await d.execute(ctx)

            if self._system_event_logger is not None:
                self._system_event_logger.log(
                    SystemEvent.PostTableMovementCompleted, "athena"
                )

    async def _enforce_table_diff_additions(self, diff: TableDiff) -> None:
        # Unload table to S3
        table_name = diff.table_name()
        temp_s3_path = f"transition/{table_name}/{table_name}.tbl"
        await self._unload_table(table_name, temp_s3_path)

        # Load it to new engines
        table_loading_awaitables = []
        for e in diff.added_locations():
            table_loading_awaitables.append(
                self._load_table_to_engine(diff.table_name(), e)
            )
        await asyncio.gather(*table_loading_awaitables)

        # Delete table-related files from S3
        ctx = self._new_execution_context()
        s3_path_list = ctx.s3_client().list_objects_v2(
            Bucket=ctx.s3_bucket(),
            Prefix=ctx.s3_path() + ("/").join(temp_s3_path.split("/")[:-1]),
        )
        d = DeleteS3Objects(
            [obj["Key"] for obj in s3_path_list["Contents"]], paths_are_relative=False
        )
        await d.execute(ctx)

    async def _unload_table(self, table_name: str, s3_path: str) -> None:
        nonsilent_assert(self._curr_blueprint is not None)
        assert self._curr_blueprint is not None
        curr_locations = self._curr_blueprint.get_table_locations(table_name)

        # TODO: The logic here assumes that all engines have the most recent version.
        if Engine.Redshift in curr_locations:  # Faster to write out from Redshift
            u = UnloadToS3(table_name, s3_path, engine=Engine.Redshift, delimiter=",")
            ctx = self._new_execution_context()
            logger.info(
                "In transition: table %s being written to S3 from Redshift.", table_name
            )
            await u.execute(ctx)
        elif Engine.Aurora in curr_locations:
            u = UnloadToS3(table_name, s3_path, engine=Engine.Aurora, delimiter=",")
            ctx = self._new_execution_context()
            logger.info(
                "In transition: table %s being written to S3 from Aurora.", table_name
            )
            await u.execute(ctx)
        elif Engine.Athena in curr_locations:
            u = UnloadToS3(table_name, s3_path, engine=Engine.Athena)
            ctx = self._new_execution_context()
            logger.info(
                "In transition: table %s being written to S3 from Athena.", table_name
            )
            await u.execute(ctx)
        else:
            logger.error(
                "In transition: table %s does not exist on any engine in current blueprint.",
                table_name,
            )

    async def _load_table_to_engine(self, table_name: str, e: Engine) -> None:
        ctx = self._new_execution_context()

        s3_path_prefix = f"transition/{table_name}/"

        # N.B: The queries we use in LoadFromS3 have different semantics per engine:
        # - The redshift query will load all files with the specified prefix. This is desired,
        #       since redshift may have dumped the table into multiple files with same prefix.
        # - The aurora query will load only from an exact match, so we compensate by creating a list
        #       of files to load from and loading from each of them.
        # - The athena query wants the directory name without the file name.

        if e == Engine.Aurora:
            # Load table to aurora from S3
            logger.info(
                "In transition: loading table %s into Aurora from S3", table_name
            )
            response = ctx.s3_client().list_objects_v2(
                Bucket=ctx.s3_bucket(), Prefix=ctx.s3_path() + s3_path_prefix
            )
            relative_paths = [
                (s3_path_prefix + obj["Key"].split("/")[-1])
                for obj in response.get("Contents", [])
            ]
            columns = self._blueprint_mgr.get_blueprint().get_table(table_name).columns
            for path in relative_paths:
                l = LoadFromS3(
                    source_table_name(table_name),
                    path,
                    e,
                    delimiter=",",
                    header_rows=1,
                    aurora_columns=comma_separated_column_names(columns),
                )
                await l.execute(ctx)
            nonsilent_assert(self._cxns is not None)
            assert self._cxns is not None
            self._cxns.get_connection(Engine.Aurora).cursor_sync().commit_sync()

            # Ensure that extract table is set up correctly for later syncs.
            nonsilent_assert(self._curr_blueprint is not None)
            assert self._curr_blueprint is not None
            table = self._curr_blueprint.get_table(table_name)
            q = "SELECT MAX({}) FROM {}".format(
                AURORA_SEQ_COLUMN, source_table_name(table)
            )
            logger.debug("Running on Aurora %s", q)
            cursor = ctx.aurora().cursor()
            await cursor.execute(q)
            row = await cursor.fetchone()
            max_seq = row[0]
            if max_seq is not None:
                q = "UPDATE {} SET next_extract_seq = {} WHERE table_name = '{}'".format(
                    AURORA_EXTRACT_PROGRESS_TABLE_NAME, max_seq + 1, table_name
                )
                logger.debug("Running on Aurora %s", q)
                await cursor.execute(q)
            await cursor.commit()

        elif e == Engine.Redshift:
            logger.info(
                "In transition: loading table %s into Redshift from S3", table_name
            )
            l = LoadFromS3(table_name, s3_path_prefix, e, delimiter=",", header_rows=1)
            await l.execute(ctx)
            nonsilent_assert(self._cxns is not None)
            assert self._cxns is not None
            self._cxns.get_connection(Engine.Redshift).cursor_sync().commit_sync()
        elif e == Engine.Athena:
            logger.info(
                "In transition: loading table %s into Athena from S3", table_name
            )
            l = LoadFromS3(table_name, s3_path_prefix, e, delimiter=",", header_rows=1)
            await l.execute(ctx)

        logger.debug(f"In transition: table {table_name} loaded to {e}.")

    def _new_execution_context(self) -> ExecutionContext:
        nonsilent_assert(self._cxns is not None)
        assert self._cxns is not None
        return ExecutionContext(
            aurora=self._cxns.get_connection_if_exists(Engine.Aurora),
            athena=self._cxns.get_connection_if_exists(Engine.Athena),
            redshift=self._cxns.get_connection_if_exists(Engine.Redshift),
            blueprint=self._blueprint_mgr.get_blueprint(),
            config=self._config,
        )


# Note that `version` is the blueprint version when the instance was created. It
# may not represent the current blueprint version.
_AURORA_PRIMARY_FORMAT = "{cluster_id}-primary-{version}"
_AURORA_REPLICA_FORMAT = "{cluster_id}-replica-{index}-{version}"
