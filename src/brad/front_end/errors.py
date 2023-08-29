class QueryError(Exception):
    @classmethod
    def from_exception(cls, ex: Exception, is_transient: bool = False) -> "QueryError":
        return cls(repr(ex), is_transient)

    def __init__(self, message: str, is_transient: bool) -> None:
        self._message = message
        self._is_transient = is_transient

    def is_transient(self) -> bool:
        """
        If the error is transient, the client should retry the query.
        """
        return self._is_transient

    def __repr__(self):
        return self._message
