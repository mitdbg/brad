import pathlib
from typing import List, Optional
from brad.vdbe.models import (
    VirtualInfrastructure,
    VirtualEngine,
    CreateVirtualEngineArgs,
)


class VdbeManager:
    """
    Used to manage the current VDBE state.
    """

    @classmethod
    def load_from(
        cls, serialized_infra_json: pathlib.Path, starting_port: int
    ) -> "VdbeManager":
        with open(serialized_infra_json, "r", encoding="utf-8") as f:
            infra = VirtualInfrastructure.model_validate_json(f.read())
        hostname = _get_hostname()
        return cls(infra, hostname, starting_port)

    def __init__(
        self, infra: VirtualInfrastructure, hostname: Optional[str], starting_port: int
    ) -> None:
        self._infra = infra
        self._hostname = hostname
        self._next_port = starting_port
        self._next_id = 1
        for engine in self._infra.engines:
            self._next_id = max(self._next_id, engine.internal_id)
        self._next_id += 1

        if self._hostname is not None:
            for engine in self._infra.engines:
                if engine.endpoint is None:
                    engine.endpoint = f"{self._hostname}:{self._assign_port()}"

    def infra(self) -> VirtualInfrastructure:
        return self._infra

    def engines(self) -> List[VirtualEngine]:
        return self._infra.engines

    def add_engine(self, create: CreateVirtualEngineArgs) -> VirtualEngine:
        engine = VirtualEngine(
            internal_id=self._next_id,
            name=create.name,
            max_staleness_ms=create.max_staleness_ms,
            p90_latency_slo_ms=create.p90_latency_slo_ms,
            interface=create.interface,
            tables=create.tables,
            mapped_to=create.mapped_to,
            endpoint=None,
        )
        self._next_id += 1

        if self._hostname is not None:
            engine.endpoint = f"{self._hostname}:{self._assign_port()}"

        self._infra.engines.append(engine)
        return engine

    def update_engine(self, engine: VirtualEngine) -> VirtualEngine:
        if engine.endpoint is None and self._hostname is not None:
            engine.endpoint = f"{self._hostname}:{self._assign_port()}"

        for i in range(len(self._infra.engines)):
            if self._infra.engines[i].internal_id == engine.internal_id:
                self._infra.engines[i] = engine
                return engine
        raise ValueError(f"Engine with id {engine.internal_id} not found")

    def delete_engine(self, engine_id: int) -> None:
        for engine in self._infra.engines:
            if engine.internal_id == engine_id:
                self._infra.engines.remove(engine)
                return
        raise ValueError(f"Engine with id {engine_id} not found")

    def _assign_port(self) -> int:
        port = self._next_port
        self._next_port += 1
        return port


def _get_hostname() -> str:
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.settimeout(0)
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()
