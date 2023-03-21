from typing import AsyncIterable
from brad.config.session import SessionId


class BradInterface:
    async def start_session(self) -> SessionId:
        raise NotImplementedError

    def run_query(self, _session_id: SessionId, _query: str) -> AsyncIterable[bytes]:
        """
        Each chunk of bytes is supposed to represent a row. We leave it in
        encoded form for simplicity.

        This method may throw an error to indicate a problem with the query.
        """
        raise NotImplementedError

    async def end_session(self, _session_id: SessionId) -> None:
        raise NotImplementedError
