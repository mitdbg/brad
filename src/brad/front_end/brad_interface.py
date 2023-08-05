from typing import AsyncIterable, Any, Dict
from brad.config.session import SessionId


class BradInterface:
    async def start_session(self) -> SessionId:
        raise NotImplementedError

    def run_query(
        self, session_id: SessionId, query: str, debug_info: Dict[str, Any]
    ) -> AsyncIterable[bytes]:
        """
        Each chunk of bytes is supposed to represent a row. We leave it in
        encoded form for simplicity.

        This method may throw an error to indicate a problem with the query.
        """
        raise NotImplementedError

    async def run_query_json(
        self, session_id: SessionId, query: str, debug_info: Dict[str, Any]
    ) -> str:
        """
        Returns query results encoded as a JSON string. Note that clients may
        need to parse non-string data types (e.g., integers).

        This method may throw an error to indicate a problem with the query.
        """
        raise NotImplementedError

    async def end_session(self, _session_id: SessionId) -> None:
        raise NotImplementedError
