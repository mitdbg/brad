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

    def __init__(self, txn_completions_per_s: float) -> None:
        # We will need to include a "worker id" once we have multiple front ends.
        self.txn_completions_per_s = txn_completions_per_s


class ShutdownFrontEnd:
    """
    Sent from the daemon to the front end indicating that it should shut down.
    """


class Sentinel:
    """
    Used when shutting down the server to unblock threads waiting for a message
    from the daemon.
    """
