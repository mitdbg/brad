import pathlib
from typing import List, Optional, Callable, Awaitable
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
        cls,
        serialized_infra_json: pathlib.Path,
        starting_port: int,
        apply_infra: Callable[[VirtualInfrastructure], Awaitable[None]],
    ) -> "VdbeManager":
        with open(serialized_infra_json, "r", encoding="utf-8") as f:
            infra = VirtualInfrastructure.model_validate_json(f.read())
        hostname = _get_hostname()
        return cls(infra, hostname, starting_port, apply_infra)

    def __init__(
        self,
        infra: VirtualInfrastructure,
        hostname: Optional[str],
        starting_port: int,
        apply_infra: Callable[[VirtualInfrastructure], Awaitable[None]],
    ) -> None:
        self._infra = infra
        self._hostname = hostname
        self._apply_infra = apply_infra
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

    async def add_engine(self, create: CreateVirtualEngineArgs) -> VirtualEngine:
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
        await self._apply_infra(self._infra)
        return engine

    async def update_engine(self, engine: VirtualEngine) -> VirtualEngine:
        if engine.endpoint is None and self._hostname is not None:
            engine.endpoint = f"{self._hostname}:{self._assign_port()}"

        for i in range(len(self._infra.engines)):
            if self._infra.engines[i].internal_id == engine.internal_id:
                self._infra.engines[i] = engine
                await self._apply_infra(self._infra)
                return engine
        raise ValueError(f"Engine with id {engine.internal_id} not found")

    async def delete_engine(self, engine_id: int) -> None:
        for engine in self._infra.engines:
            if engine.internal_id == engine_id:
                self._infra.engines.remove(engine)
                await self._apply_infra(self._infra)
                return
        raise ValueError(f"Engine with id {engine_id} not found")

    def _assign_port(self) -> int:
        port = self._next_port
        self._next_port += 1
        return port


class VdbeFrontEndManager:
    """
    Used on the front end. Provides a read-only view of the current VDBE state.
    """

    def __init__(self, initial_infra: VirtualInfrastructure) -> None:
        self._infra = initial_infra

    def engines(self) -> List[VirtualEngine]:
        return self._infra.engines

    def engine_by_id(self, engine_id: int) -> Optional[VirtualEngine]:
        for engine in self._infra.engines:
            if engine.internal_id == engine_id:
                return engine
        return None

    def update_infra(self, new_infra: VirtualInfrastructure) -> None:
        self._infra = new_infra


def _get_hostname() -> str:
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.settimeout(0)
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()
