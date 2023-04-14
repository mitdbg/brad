from typing import Optional

from ..provisioning import Provisioning


class ProvisioningDiff:
    """
    Indicates changes in a provisioning. A `None` property indicates that there
    was no change to it.
    """

    @classmethod
    def of(cls, old: Provisioning, new: Provisioning) -> Optional["ProvisioningDiff"]:
        if (
            old.instance_type() == new.instance_type()
            and old.num_nodes() == new.num_nodes()
        ):
            return None
        elif old.instance_type() == new.instance_type():
            return cls(new_instance_type=None, new_num_nodes=new.num_nodes())
        elif old.num_nodes() == new.num_nodes():
            return cls(new_instance_type=new.instance_type(), new_num_nodes=None)
        else:
            return cls(
                new_instance_type=new.instance_type(), new_num_nodes=new.num_nodes()
            )

    def __init__(
        self, new_instance_type: Optional[str], new_num_nodes: Optional[int]
    ) -> None:
        self._new_instance_type = new_instance_type
        self._new_num_nodes = new_num_nodes

    def new_instance_type(self) -> Optional[str]:
        return self._new_instance_type

    def new_num_nodes(self) -> Optional[int]:
        return self._new_num_nodes
