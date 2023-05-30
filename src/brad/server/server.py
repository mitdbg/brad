import asyncio
import io
import logging
import queue
from datetime import datetime, timezone
import multiprocessing as mp
from typing import AsyncIterable, Optional
import random

import grpc
import pyodbc

import brad.proto_gen.brad_pb2_grpc as brad_grpc

from brad.blueprint import Blueprint
from brad.blueprint.diff.blueprint import BlueprintDiff
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.daemon.daemon import BradDaemon
from brad.daemon.monitor import Monitor
from brad.daemon.messages import ShutdownDaemon, NewBlueprint, Sentinel, ReceivedQuery
from brad.data_sync.execution.executor import DataSyncExecutor
from brad.provisioning.physical import PhysicalProvisioning
from brad.routing import Router
from brad.routing.always_one import AlwaysOneRouter
from brad.routing.rule_based import RuleBased
from brad.routing.location_aware_round_robin import LocationAwareRoundRobin
from brad.routing.policy import RoutingPolicy
from brad.server.brad_interface import BradInterface
from brad.server.blueprint_manager import BlueprintManager
from brad.server.epoch_file_handler import EpochFileHandler
from brad.server.errors import QueryError
from brad.server.grpc import BradGrpc
from brad.server.session import SessionManager, SessionId
from brad.query_rep import QueryRep

logger = logging.getLogger(__name__)

LINESEP = "\n".encode()


class BradServer(BradInterface):
    def __init__(
        self,
        config: ConfigFile,
        schema_name: str,
        path_to_planner_config: str,
        debug_mode: bool,
    ):
        self._config = config
        self._schema_name = schema_name
        self._debug_mode = debug_mode
        self._blueprint_mgr = BlueprintManager(self._config, self._schema_name)
        self._path_to_planner_config = path_to_planner_config
        self._monitor: Optional[Monitor] = None

        # Set up query logger
        self._qlogger = logging.getLogger("queries")
        self._qlogger.setLevel(logging.INFO)
        qhandler = EpochFileHandler(
            self._config.local_logs_path,
            self._config.epoch_length,
            self._config.s3_logs_bucket,
            self._config.s3_logs_path,
            self._config.txn_log_prob,
        )
        qhandler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(message)s")
        qhandler.setFormatter(formatter)
        self._qlogger.addHandler(qhandler)

        # We have different routing policies for performance evaluation and
        # testing purposes.
        routing_policy = self._config.routing_policy
        self.routing_policy = routing_policy
        if routing_policy == RoutingPolicy.Default:
            self._router: Router = LocationAwareRoundRobin(self._blueprint_mgr)
        elif routing_policy == RoutingPolicy.AlwaysAthena:
            self._router = AlwaysOneRouter(Engine.Athena)
        elif routing_policy == RoutingPolicy.AlwaysAurora:
            self._router = AlwaysOneRouter(Engine.Aurora)
        elif routing_policy == RoutingPolicy.AlwaysRedshift:
            self._router = AlwaysOneRouter(Engine.Redshift)
        elif routing_policy == RoutingPolicy.RuleBased:
            # TODO(Amadou): Use real constructor.
            self._monitor = Monitor.from_config_file(config)
            self._physical = PhysicalProvisioning(
                self._monitor,
                self._blueprint_mgr.get_blueprint(),
                cluster_ids=config.get_cluster_ids(),
            )
            self._router = RuleBased(
                blueprint_mgr=self._blueprint_mgr, monitor=self._monitor
            )
        else:
            raise RuntimeError(
                "Unsupported routing policy: {}".format(str(routing_policy))
            )
        conn_info = dict()
        if self._physical is not None:
            conn_info = self._physical.connection_info()
        self._sessions = SessionManager(
            self._config, self._schema_name, conn_info=conn_info
        )
        self._data_sync_executor = DataSyncExecutor(self._config, self._blueprint_mgr)
        self._timed_sync_task = None
        self._daemon_messages_task = None

        # Used for managing the daemon process.
        self._daemon_mp_manager: Optional[mp.managers.SyncManager] = None
        self._daemon_input_queue: Optional[mp.Queue] = None
        self._daemon_output_queue: Optional[mp.Queue] = None
        self._daemon_process: Optional[mp.Process] = None

    async def serve_forever(self):
        try:
            await self.run_setup()
            grpc_server = grpc.aio.server()
            brad_grpc.add_BradServicer_to_server(BradGrpc(self), grpc_server)
            grpc_server.add_insecure_port(
                "{}:{}".format(self._config.server_interface, self._config.server_port)
            )
            await grpc_server.start()
            logger.info("The BRAD server has successfully started.")
            logger.info("Listening on port %d.", self._config.server_port)

            if self.routing_policy == RoutingPolicy.RuleBased:
                assert (
                    self._monitor is not None
                ), "require monitor running for rule-based router"
                await asyncio.gather(
                    self._monitor.run_forever(), grpc_server.wait_for_termination()
                )
            else:
                await grpc_server.wait_for_termination()
        finally:
            # Not ideal, but we need to manually call this method to ensure
            # gRPC's internal shutdown process completes before we return from
            # this method.
            grpc_server.__del__()
            await self.run_teardown()
            logger.info("The BRAD server has shut down.")

    async def run_setup(self):
        await self._blueprint_mgr.load()
        if self._config.data_sync_period_seconds > 0:
            self._timed_sync_task = asyncio.create_task(self._run_sync_periodically())
        await self._data_sync_executor.establish_connections()

        # Launch the daemon process.
        self._daemon_mp_manager = mp.Manager()
        self._daemon_input_queue = self._daemon_mp_manager.Queue()
        self._daemon_output_queue = self._daemon_mp_manager.Queue()
        self._daemon_messages_task = asyncio.create_task(self._read_daemon_messages())
        self._daemon_process = mp.Process(
            target=BradDaemon.launch_in_subprocess,
            args=(
                self._config.raw_path,
                self._schema_name,
                self._blueprint_mgr.get_blueprint(),
                self._path_to_planner_config,
                self._debug_mode,
                self._daemon_input_queue,
                self._daemon_output_queue,
            ),
        )

        self._daemon_process.start()
        logger.info("The BRAD daemon process has been started.")

    async def run_teardown(self):
        await self._sessions.end_all_sessions()

        loop = asyncio.get_event_loop()
        assert self._daemon_input_queue is not None
        assert self._daemon_output_queue is not None
        assert self._daemon_process is not None

        # Tell the daemon process to shut down and wait for it to do so.
        await loop.run_in_executor(None, self._daemon_input_queue.put, ShutdownDaemon())
        await loop.run_in_executor(None, self._daemon_process.join)

        # Important for unblocking our message reader thread.
        self._daemon_output_queue.put(Sentinel())

        if self._timed_sync_task is not None:
            await self._timed_sync_task.close()
            self._timed_sync_task = None

        await self._data_sync_executor.shutdown()

    async def start_session(self, read_only: bool = False) -> SessionId:
        session_id, _ = await self._sessions.create_new_session(read_only)
        return session_id

    async def end_session(self, session_id: SessionId) -> None:
        await self._sessions.end_session(session_id)

    # pylint: disable-next=invalid-overridden-method
    async def run_query(
        self, session_id: SessionId, query: str
    ) -> AsyncIterable[bytes]:
        session = self._sessions.get_session(session_id)
        if session is None:
            raise QueryError("Invalid session id {}".format(str(session_id)))

        try:
            # Remove any trailing or leading whitespace.
            query = query.strip()

            # Handle internal commands separately.
            if query.startswith("BRAD_"):
                async for output in self._handle_internal_command(query):
                    yield output
                return

            # 2. Select an engine for the query.
            query_rep = QueryRep(query)
            engine_to_use = self._router.engine_for(query_rep)
            logger.debug("Routing '%s' to %s", query, engine_to_use)

            # 3. Actually execute the query.
            connection = session.engines.get_connection(engine_to_use)
            cursor = await connection.cursor()
            try:
                start = datetime.now(tz=timezone.utc)
                await cursor.execute(query_rep.raw_query)
                end = datetime.now(tz=timezone.utc)
            except (pyodbc.ProgrammingError, pyodbc.Error) as ex:
                # Error when executing the query.
                raise QueryError.from_exception(ex)

            # Decide whether to log the query.
            if query_rep.is_analytical_query() or (
                random.random() < self._config.txn_log_prob
            ):
                self._qlogger.info(
                    f"{end.strftime('%Y-%m-%d %H:%M:%S,%f')} INFO Query: {query} Engine: {engine_to_use} Duration: {end-start}s IsTransaction: {query_rep.is_transactional_query()}"
                )

            # Extract and return the results, if any.
            try:
                num_rows = 0
                while True:
                    row = await cursor.fetchone()
                    if row is None:
                        break
                    num_rows += 1
                    yield " | ".join(map(str, row)).encode()
                logger.debug("Responded with %d rows.", num_rows)
            except pyodbc.ProgrammingError:
                logger.debug("No rows produced.")

            if self._daemon_input_queue is not None:
                try:
                    # Send the query to the daemon. Note that the daemon needs to
                    # consume these queries quickly enough to avoid an overflow.
                    self._daemon_input_queue.put(ReceivedQuery(query), block=False)
                except queue.Full:
                    logger.warning(
                        "Daemon input queue is full. Not sending query '%s'", query
                    )

        except QueryError:
            # This is an expected exception. We catch and re-raise it here to
            # avoid triggering the handler below.
            raise
        except Exception as ex:
            logger.exception("Encountered unexpected exception when handling request.")
            raise QueryError.from_exception(ex)

    async def _handle_internal_command(self, command_raw: str) -> AsyncIterable[bytes]:
        """
        This method is used to handle BRAD_ prefixed "queries" (i.e., commands
        to run custom functionality like syncing data across the engines).
        """
        # Clean up the command. We remove the trailing semicolon (;), remove any
        # whitespace, and capitalize the command.
        command = command_raw[:-1].strip().upper()

        if command == "BRAD_SYNC":
            logger.debug("Manually triggered a data sync.")
            ran_sync = await self._data_sync_executor.run_sync(
                self._blueprint_mgr.get_blueprint()
            )
            if ran_sync:
                yield "Sync succeeded.".encode()
            else:
                yield "Sync skipped. No new writes to sync.".encode()

        elif command == "BRAD_EXPLAIN_SYNC_STATIC":
            logical_plan = self._data_sync_executor.get_static_logical_plan(
                self._blueprint_mgr.get_blueprint()
            )
            out = io.StringIO()
            logical_plan.print_plan_sequentially(file=out)
            yield out.getvalue().encode()

        elif command == "BRAD_EXPLAIN_SYNC":
            logical, physical = await self._data_sync_executor.get_processed_plans(
                self._blueprint_mgr.get_blueprint()
            )
            out = io.StringIO()
            logical.print_plan_sequentially(file=out)
            yield out.getvalue().encode()

            out = io.StringIO()
            physical.print_plan_sequentially(file=out)
            yield out.getvalue().encode()

        else:
            yield "Unknown internal command: {}".format(command).encode()

    async def _run_sync_periodically(self):
        while True:
            await asyncio.sleep(self._config.data_sync_period_seconds)
            logger.debug("Starting an auto data sync.")
            await self._data_sync_executor.run_sync(self._blueprint_mgr.get_blueprint())

    async def _read_daemon_messages(self) -> None:
        assert self._daemon_output_queue is not None
        loop = asyncio.get_running_loop()
        while True:
            message = await loop.run_in_executor(None, self._daemon_output_queue.get)

            if isinstance(message, NewBlueprint):
                await self._handle_new_blueprint(message.blueprint)

    async def _handle_new_blueprint(self, new_blueprint: Blueprint) -> None:
        # This is where we launch any reconfigurations needed to realize
        # the new blueprint.
        logger.debug("Received new blueprint: %s", new_blueprint)
        curr_blueprint = self._blueprint_mgr.get_blueprint()
        _diff = BlueprintDiff.of(curr_blueprint, new_blueprint)

        # - Provisioning changes handled here (if there are blueprint changes).
        # - Need to update the blueprint stored in the manager.
