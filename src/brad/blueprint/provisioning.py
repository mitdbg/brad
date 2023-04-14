class Provisioning:
    def __init__(self, instance_type: str, num_nodes: int) -> None:
        self._instance_type = instance_type
        self._num_nodes = num_nodes

    def instance_type(self) -> str:
        return self._instance_type

    def num_nodes(self) -> int:
        return self._num_nodes
