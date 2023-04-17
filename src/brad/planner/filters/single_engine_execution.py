from .filter import Filter


class SingleEngineExecution(Filter):
    """
    Ensures that all tables referenced by a query are present together on at
    least one engine.
    """
