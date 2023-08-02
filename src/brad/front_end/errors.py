class QueryError(Exception):
    @classmethod
    def from_exception(cls, ex: Exception) -> "QueryError":
        return cls(repr(ex))

    def __init__(self, message: str):
        self._message = message

    def __repr__(self):
        return self._message
