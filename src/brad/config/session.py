class SessionId:
    def __init__(self, id_value: int):
        self._session_id = id_value

    def __repr__(self) -> str:
        return str(self._session_id)

    def value(self) -> int:
        """
        Meant for serialization only.
        """
        return self._session_id

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SessionId):
            return False
        return self._session_id == other._session_id

    def __hash__(self) -> int:
        return hash(self._session_id)
