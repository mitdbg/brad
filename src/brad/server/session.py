import asyncio
import logging
from typing import Dict, Tuple, Optional

from brad.config.file import ConfigFile
from brad.config.session import SessionId
from .engine_connections import EngineConnections

logger = logging.getLogger(__name__)


class Session:
    """
    Stores session-specific state (on the server). Each session has its own
    connections to the underlying engines. Create instances using
    `SessionManager`.
    """

    def __init__(self, session_id: SessionId, engines: EngineConnections):
        self._session_id = session_id
        self._engines = engines
        self._in_txn = False

    @property
    def identifier(self) -> SessionId:
        return self._session_id

    @property
    def engines(self) -> EngineConnections:
        return self._engines

    @property
    def in_transaction(self) -> bool:
        return self._in_txn

    def set_in_transaction(self, in_txn: bool) -> None:
        self._in_txn = in_txn

    async def close(self):
        await self._engines.close()


class SessionManager:
    def __init__(self, config: ConfigFile, schema_name: str):
        self._config = config
        self._next_id_value = 0
        self._sessions: Dict[SessionId, Session] = {}
        # Eventually we will allow connections to multiple underlying "schemas"
        # (table namespaces).  There is no fundamental reason why we cannot
        # support this - it's just unnecessary for the early stages of this
        # project. For now we assume that we always operate against one schema
        # and that it is provided up front when starting BRAD.
        self._schema_name = schema_name

    async def create_new_session(self) -> Tuple[SessionId, Session]:
        logger.debug("Creating a new session...")
        session_id = SessionId(self._next_id_value)
        self._next_id_value += 1
        connections = await EngineConnections.connect(
            self._config,
            self._schema_name,
        )
        session = Session(session_id, connections)
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
