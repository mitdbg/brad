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

    def __init__(
        self, session_id: SessionId, engines: EngineConnections, database_name: str
    ):
        self._session_id = session_id
        self._engines = engines
        self._database_name = database_name

    @property
    def identifier(self) -> SessionId:
        return self._session_id

    @property
    def engines(self) -> EngineConnections:
        return self._engines

    @property
    def database_name(self) -> str:
        return self._database_name

    async def close(self):
        await self._engines.close()


class SessionManager:
    def __init__(self, config: ConfigFile):
        self._config = config
        self._next_id_value = 0
        self._sessions: Dict[SessionId, Session] = {}

    async def create_new_session(self, database_name: str) -> Tuple[SessionId, Session]:
        logger.debug("Creating a new session for database '%s'...", database_name)
        session_id = SessionId(self._next_id_value)
        self._next_id_value += 1
        connections = await EngineConnections.connect(self._config, database_name)
        session = Session(session_id, connections, database_name)
        self._sessions[session_id] = session
        logger.debug("Established a new session: (%s, %s)", session_id, database_name)
        return (session_id, session)

    def get_session(self, session_id: SessionId) -> Optional[Session]:
        if session_id not in self._sessions:
            return None
        return self._sessions[session_id]

    async def end_session(self, session_id: SessionId):
        session = self._sessions[session_id]
        await session.close()
        logger.debug("Ended session %s", session_id)
        del self._sessions[session_id]
