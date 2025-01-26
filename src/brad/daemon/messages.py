from typing import Tuple, List
from ddsketch import DDSketch
from ddsketch.pb.proto import DDSketchProto, pb as ddspb

from brad.provisioning.directory import Directory
from brad.row_list import RowList
from brad.vdbe.models import VirtualInfrastructure


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

    def __init__(
        self, fe_index: int, version: int, updated_directory: Directory
    ) -> None:
        super().__init__(fe_index)
        self.version = version
        self.updated_directory = updated_directory


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

    @classmethod
    def from_data(
        cls,
        fe_index,
        txn_completions_per_s: float,
        txn_latency_sketch: DDSketch,
        query_latency_sketch: DDSketch,
    ) -> "MetricsReport":
        return cls(
            fe_index,
            txn_completions_per_s,
            serialized_txn_latency_sketch=DDSketchProto.to_proto(
                txn_latency_sketch
            ).SerializeToString(),
            serialized_query_latency_sketch=DDSketchProto.to_proto(
                query_latency_sketch
            ).SerializeToString(),
        )

    def __init__(
        self,
        fe_index: int,
        txn_completions_per_s: float,
        serialized_txn_latency_sketch: bytes,
        serialized_query_latency_sketch: bytes,
    ) -> None:
        super().__init__(fe_index)
        self.txn_completions_per_s = txn_completions_per_s
        self.serialized_txn_latency_sketch = serialized_txn_latency_sketch
        self.serialized_query_latency_sketch = serialized_query_latency_sketch

    def txn_latency_sketch(self) -> DDSketch:
        pb_sketch = ddspb.DDSketch()
        pb_sketch.ParseFromString(self.serialized_txn_latency_sketch)
        return DDSketchProto.from_proto(pb_sketch)

    def query_latency_sketch(self) -> DDSketch:
        pb_sketch = ddspb.DDSketch()
        pb_sketch.ParseFromString(self.serialized_query_latency_sketch)
        return DDSketchProto.from_proto(pb_sketch)


class VdbeMetricsReport(IpcMessage):
    """
    Sent from the VDBE front end to the daemon to report BRAD's client-side metrics.
    """

    @classmethod
    def from_data(
        cls,
        fe_index: int,
        latency_sketches: List[Tuple[int, DDSketch]],
    ) -> "VdbeMetricsReport":
        serialized_sketches = [
            (vdbe_id, DDSketchProto.to_proto(sketch).SerializeToString())
            for vdbe_id, sketch in latency_sketches
        ]
        return cls(fe_index, latency_sketches=serialized_sketches)

    def __init__(
        self,
        fe_index: int,
        latency_sketches: List[Tuple[int, bytes]],
    ) -> None:
        super().__init__(fe_index)
        self.serialized_latency_sketches = latency_sketches

    def query_latency_sketches(self) -> List[Tuple[int, DDSketch]]:
        results = []
        for vdbe_id, serialized_sketch in self.serialized_latency_sketches:
            pb_sketch = ddspb.DDSketch()
            pb_sketch.ParseFromString(serialized_sketch)
            results.append((vdbe_id, DDSketchProto.from_proto(pb_sketch)))
        return results


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


class ReconcileVirtualInfrastructure(IpcMessage):
    """
    Sent from the daemon to the VDBE front end to update its virtual infrastructure.
    """

    def __init__(self, fe_index: int, virtual_infra: VirtualInfrastructure) -> None:
        super().__init__(fe_index)
        self.virtual_infra = virtual_infra


class ReconcileVirtualInfrastructureAck(IpcMessage):
    """
    Sent from the VDBE front end back to the daemon to acknowledge the virtual infrastructure update.
    """

    def __init__(self, fe_index: int, num_added: int, num_removed: int) -> None:
        super().__init__(fe_index)
        self.num_added = num_added
        self.num_removed = num_removed


class ShutdownFrontEnd(IpcMessage):
    """
    Sent from the daemon to the front end indicating that it should shut down.
    """


class Sentinel(IpcMessage):
    """
    Used when shutting down the server to unblock threads waiting for a message
    from the daemon.
    """
