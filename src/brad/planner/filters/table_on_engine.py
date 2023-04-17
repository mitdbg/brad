from .filter import Filter


class TableOnEngine(Filter):
    """
    Ensures that a table is only placed on an engine if it has at least one node
    (or is Athena).
    """
