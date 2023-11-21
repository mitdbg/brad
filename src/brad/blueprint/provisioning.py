class Provisioning:
    def __init__(self, instance_type: str, num_nodes: int) -> None:
        self._instance_type = instance_type
        self._num_nodes = num_nodes

    def instance_type(self) -> str:
        return self._instance_type

    def num_nodes(self) -> int:
        return self._num_nodes

    def clone(self) -> "Provisioning":
        return Provisioning(self._instance_type, self._num_nodes)

    def mutable_clone(self) -> "MutableProvisioning":
        return MutableProvisioning(self._instance_type, self._num_nodes)

    def __repr__(self) -> str:
        return "".join(
            [
                self._instance_type,
                "(",
                str(self._num_nodes),
                ")",
            ]
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Provisioning):
            return False
        return (
            self._instance_type == other._instance_type
            and self._num_nodes == other._num_nodes
        )

    def __hash__(self) -> int:
        return hash((self._instance_type, self._num_nodes))


class MutableProvisioning(Provisioning):
    def set_instance_type(self, instance_type: str) -> None:
        self._instance_type = instance_type

    def set_num_nodes(self, num_nodes: int) -> None:
        self._num_nodes = num_nodes
