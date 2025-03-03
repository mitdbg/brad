import asyncio
import logging
from datetime import datetime
from typing import Dict, Tuple, Optional

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.config.session import SessionId
from brad.blueprint.manager import BlueprintManager
from brad.front_end.debug import ReestablishConnectionsReport
from brad.front_end.engine_connections import EngineConnections
from brad.planner.estimator import Estimator
from brad.routing.policy import RoutingPolicy
from brad.routing.tree_based.forest_policy import ForestPolicy
from brad.data_stats.postgres_estimator import PostgresEstimator
from brad.data_stats.stub_estimator import StubEstimator
from brad.utils.time_periods import universal_now

logger = logging.getLogger(__name__)


class Session:
    """
    Stores session-specific state (on the front end). Each session has its own
    connections to the underlying engines. Create instances using
    `SessionManager`.
    """

    def __init__(
        self,
        session_id: SessionId,
        engines: EngineConnections,
        estimator: Optional[Estimator],
    ):
        self._session_id = session_id
        self._engines = engines
        self._in_txn = False
        self._closed = False
        self._txn_start_timestamp = universal_now()
        self._estimator = estimator

    @property
    def identifier(self) -> SessionId:
        return self._session_id

    @property
    def engines(self) -> EngineConnections:
        return self._engines

    @property
    def in_transaction(self) -> bool:
        return self._in_txn

    @property
    def estimator(self) -> Optional[Estimator]:
        return self._estimator

    @property
    def closed(self) -> bool:
        return self._closed

    def set_in_transaction(self, in_txn: bool) -> None:
        self._in_txn = in_txn

    def set_txn_start_timestamp(self, timestamp: datetime) -> None:
        self._txn_start_timestamp = timestamp

    def txn_start_timestamp(self) -> datetime:
        return self._txn_start_timestamp

    async def close(self):
        self._closed = True
        await self._engines.close()
        if self._estimator is not None:
            await self._estimator.close()


class SessionManager:
    def __init__(
        self,
        config: ConfigFile,
        blueprint_mgr: "BlueprintManager",
        schema_name: str,
        for_vdbes: bool = False,
    ) -> None:
        self._config = config
        self._blueprint_mgr = blueprint_mgr
        self._next_id_value = 0
        self._sessions: Dict[SessionId, Session] = {}
        # Eventually we will allow connections to multiple underlying "schemas"
        # (table namespaces).  There is no fundamental reason why we cannot
        # support this - it's just unnecessary for the early stages of this
        # project. For now we assume that we always operate against one schema
        # and that it is provided up front when starting BRAD.
        self._schema_name = schema_name
        self._for_vdbes = for_vdbes

    async def create_new_session(self) -> Tuple[SessionId, Session]:
        logger.debug("Creating a new session...")
        session_id = SessionId(self._next_id_value)
        self._next_id_value += 1

        # Only connect to running engines.
        engines = {Engine.Athena}
        blueprint = self._blueprint_mgr.get_blueprint()
        if blueprint.aurora_provisioning().num_nodes() > 0:
            engines.add(Engine.Aurora)
        if blueprint.redshift_provisioning().num_nodes() > 0:
            engines.add(Engine.Redshift)

        connections = await EngineConnections.connect(
            self._config,
            self._blueprint_mgr.get_directory(),
            self._schema_name,
            specific_engines=engines,
            connect_to_aurora_read_replicas=True,
        )

        # Create an estimator if needed. The estimator should be
        # session-specific since it currently depends on a DB connection.
        routing_policy_override = self._config.routing_policy
        if not self._for_vdbes and (
            routing_policy_override == RoutingPolicy.ForestTableSelectivity
            or routing_policy_override == RoutingPolicy.Default
        ):
            policy = blueprint.get_routing_policy()
            requires_estimator = isinstance(policy.definite_policy, ForestPolicy)
            if self._config.stub_mode_path() is None and requires_estimator:
                estimator: Optional[Estimator] = await PostgresEstimator.connect(
                    self._schema_name, self._config
                )
            else:
                estimator = StubEstimator()
            assert estimator is not None
            await estimator.analyze(self._blueprint_mgr.get_blueprint())
        else:
            estimator = None

        session = Session(session_id, connections, estimator)
        self._sessions[session_id] = session
        logger.debug("Established a new session: %s", session_id)
        return (session_id, session)

    def get_session(self, session_id: SessionId) -> Optional[Session]:
        if session_id not in self._sessions:
            return None
        return self._sessions[session_id]

    async def end_session(self, session_id: SessionId) -> None:
        session = self._sessions[session_id]
        await session.close()
        logger.debug("Ended session %s", session_id)
        del self._sessions[session_id]

    async def end_all_sessions(self) -> None:
        logger.debug("Ending all remaining sessions...")
        end_tasks = []
        for session_id, session in self._sessions.items():
            end_tasks.append(session.close())
            logger.debug("Ended session %s", session_id)
        await asyncio.gather(*end_tasks)
        self._sessions.clear()

    async def add_and_refresh_connections(self) -> None:
        """
        Used during blueprint transitions to add connections to newly started
        engines.
        """
        blueprint = self._blueprint_mgr.get_blueprint()
        directory = self._blueprint_mgr.get_directory()

        expected_engines = {Engine.Athena}
        if blueprint.aurora_provisioning().num_nodes() > 0:
            expected_engines.add(Engine.Aurora)
        if blueprint.redshift_provisioning().num_nodes() > 0:
            expected_engines.add(Engine.Redshift)

        # Important to copy and iterate over the sessions because the dictionary
        # could be modified during the await. This is OK because the blueprint
        # change is "atomic" - new sessions arriving after the blueprint are
        # refreshed will connect to the new instances. Another blueprint change
        # cannot happen until this is complete (because we notify the daemon).
        existing_sessions = [session for session in self._sessions.values()]
        for session in existing_sessions:
            if session.closed:
                continue
            await session.engines.add_and_refresh_connections(
                self._config, directory, expected_engines
            )

    async def remove_connections(self) -> None:
        """
        Used during blueprint transitions to remove connections to stopped engines.
        """
        blueprint = self._blueprint_mgr.get_blueprint()

        expected_engines = {Engine.Athena}
        expected_aurora_read_replicas = 0
        if blueprint.aurora_provisioning().num_nodes() > 0:
            expected_engines.add(Engine.Aurora)
            expected_aurora_read_replicas = (
                blueprint.aurora_provisioning().num_nodes() - 1
            )
        if blueprint.redshift_provisioning().num_nodes() > 0:
            expected_engines.add(Engine.Redshift)

        # See the comment in `add_and_refresh_connections()`.
        existing_sessions = [session for session in self._sessions.values()]
        for session in existing_sessions:
            if session.closed:
                continue
            await session.engines.remove_connections(
                expected_engines, expected_aurora_read_replicas
            )

    async def reestablish_connections(self) -> ReestablishConnectionsReport:
        """
        Used to reconnect to engines when a connection has been lost. Lost
        connections may occur during blueprint transitions when the provisioning
        changes (e.g., an Aurora failover). This method returns `True` iff all
        of the lost connections were re-established.

        Callers should take care to not call this method repeatedly to avoid
        overwhelming the underlying engines. Use randomized exponential backoff
        instead.
        """
        logger.debug("Attempting to reestablish connections...")
        directory = self._blueprint_mgr.get_directory()
        sessions = [session for session in self._sessions.values()]
        overall_report = ReestablishConnectionsReport()
        for session in sessions:
            if session.closed:
                continue

            report = await session.engines.reestablish_connections(
                self._config, directory
            )
            if session.closed:
                continue
            overall_report.merge(report)
            # Continue running since we still want to try connecting other
            # sessions.
        logger.debug(
            "Reestablish connections succeeded? %s", str(overall_report.all_succeeded())
        )
        return overall_report
