from typing import Dict, Tuple

from brad.config.file import ConfigFile
from .engine_connections import EngineConnections


class SessionId:
    def __init__(self, id_value: int):
        self._session_id = id_value


class Session:
    """
    Stores session-specific state. Each session has its own connections to the
    underlying engines. Create instances using `SessionManager`.
    """

    def __init__(self, session_id: SessionId, engines: EngineConnections):
        self._session_id = session_id
        self._engines = engines

    @property
    def identifier(self) -> SessionId:
        return self._session_id

    @property
    def engines(self) -> EngineConnections:
        return self._engines


class SessionManager:
    def __init__(self, config: ConfigFile):
        self._config = config
        self._next_id_value = 0
        self._sessions: Dict[SessionId, Session] = {}

    async def create_new_session(self) -> Tuple[SessionId, Session]:
        session_id = SessionId(self._next_id_value)
        self._next_id_value += 1
        connections = await EngineConnections.connect(self._config)
        session = Session(session_id, connections)
        self._sessions[session_id] = session
        return (session_id, session)

    def get_session(self, session_id: SessionId) -> Session:
        return self._sessions[session_id]

    def end_session(self, session_id: SessionId):
        del self._sessions[session_id]
