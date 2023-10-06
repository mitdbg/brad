from brad.row_list import RowList
from brad.utils.run_time_reservoir import RunTimeSummary


class IpcMessage:
    """
    Base class representing a generic message sent between the daemon and front
    ends.
    """

    def __init__(self, fe_index: int) -> None:
        # The front end server for the message.
        self.fe_index = fe_index


class NewBlueprint(IpcMessage):
    """
    Sent from the daemon to the front end indicating that there is a new
    blueprint.
    """

    def __init__(self, fe_index: int, version: int) -> None:
        super().__init__(fe_index)
        self.version = version


class NewBlueprintAck(IpcMessage):
    """
    Sent from the front end back to the server to indicate that it has
    transitioned to the new blueprint.
    """

    def __init__(self, fe_index: int, version: int) -> None:
        super().__init__(fe_index)
        self.version = version


class MetricsReport(IpcMessage):
    """
    Sent from the front end to the daemon to report BRAD's client-side metrics.
    """

    def __init__(
        self, fe_index: int, txn_completions_per_s: float, latency: RunTimeSummary
    ) -> None:
        super().__init__(fe_index)
        self.txn_completions_per_s = txn_completions_per_s
        self.latency = latency


class InternalCommandRequest(IpcMessage):
    """
    Sent from the front end to the daemon to handle an internal command.
    """

    def __init__(self, fe_index: int, request: str) -> None:
        super().__init__(fe_index)
        self.request = request


class InternalCommandResponse(IpcMessage):
    """
    Sent from the daemon to the front end to respond to an `InternalCommandRequest`.
    """

    def __init__(self, fe_index: int, response: RowList) -> None:
        super().__init__(fe_index)
        self.response = response


class ShutdownFrontEnd(IpcMessage):
    """
    Sent from the daemon to the front end indicating that it should shut down.
    """


class Sentinel(IpcMessage):
    """
    Used when shutting down the server to unblock threads waiting for a message
    from the daemon.
    """
