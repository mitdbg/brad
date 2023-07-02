from .cursor import Cursor


class Connection:
    """
    A wrapper class used to hide the different connection implementations for
    each engine and to unify the async/sync interface. This class also helps
    with type checking.
    """

    def cursor(self) -> Cursor:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError
