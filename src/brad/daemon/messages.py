from brad.blueprint import Blueprint


class NewBlueprint:
    """
    Sent from the daemon to the server indicating that there is a new blueprint.
    """

    def __init__(self, blueprint: Blueprint) -> None:
        self.blueprint = blueprint


class MetricsReport:
    """
    Sent from the server to the daemon to report BRAD's client-side metrics.
    """

    def __init__(self, txn_end_value: int, elapsed_time_s: float) -> None:
        self.txn_end_value = txn_end_value
        self.elapsed_time_s = elapsed_time_s


class ShutdownDaemon:
    """
    Sent from the server to the daemon indicating that it should shut down.
    """


class Sentinel:
    """
    Used when shutting down the server to unblock threads waiting for a message
    from the daemon.
    """
