import pathlib
from typing import List, Optional
from brad.vdbe.models import VirtualInfrastructure, VirtualEngine


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

        if self._hostname is not None:
            for engine in self._infra.engines:
                if engine.endpoint is None:
                    engine.endpoint = f"{self._hostname}:{self._assign_port()}"

    def infra(self) -> VirtualInfrastructure:
        return self._infra

    def engines(self) -> List[VirtualEngine]:
        return self._infra.engines

    def add_update_engine(self, engine: VirtualEngine) -> VirtualEngine:
        if engine.endpoint is None and self._hostname is not None:
            engine.endpoint = f"{self._hostname}:{self._assign_port()}"

        for i in range(len(self._infra.engines)):
            if self._infra.engines[i].name == engine.name:
                self._infra.engines[i] = engine
                return engine
        self._infra.engines.append(engine)
        return engine

    def delete_engine(self, engine_name: str) -> bool:
        for engine in self._infra.engines:
            if engine.name == engine_name:
                self._infra.engines.remove(engine)
                return True
        return False

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
