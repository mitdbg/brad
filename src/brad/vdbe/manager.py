import pathlib
from typing import List
from brad.vdbe.models import VirtualInfrastructure, VirtualEngine


class VdbeManager:
    """
    Used to manage the current VDBE state.
    """

    @classmethod
    def load_from(cls, serialized_infra_json: pathlib.Path) -> "VdbeManager":
        with open(serialized_infra_json, "r", encoding="utf-8") as f:
            infra = VirtualInfrastructure.model_validate_json(f.read())
        return cls(infra)

    def __init__(self, infra: VirtualInfrastructure) -> None:
        self._infra = infra

    def infra(self) -> VirtualInfrastructure:
        return self._infra

    def engines(self) -> List[VirtualEngine]:
        return self._infra.engines
