from brad.blueprint import Blueprint


class NewBlueprint:
    """
    Sent from the daemon to the server indicating that there is a new blueprint.
    """

    def __init__(self, blueprint: Blueprint) -> None:
        self.blueprint = blueprint


class ReceivedQuery:
    """
    Sent from the server to the daemon with a query that it received.
    """

    def __init__(self, query_str: str) -> None:
        self.query_str = query_str


class ShutdownDaemon:
    """
    Sent from the server to the daemon indicating that it should shut down.
    """
