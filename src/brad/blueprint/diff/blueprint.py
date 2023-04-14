from typing import List, Optional

from ..blueprint import Blueprint
from .provisioning import ProvisioningDiff
from .table import TableDiff


class BlueprintDiff:
    @classmethod
    def of(cls, old: Blueprint, new: Blueprint) -> Optional["BlueprintDiff"]:
        # This code assumes that there are no table additions/removals, which we
        # do not currently support.
        old_table_locations = {tbl.name: set(tbl.locations) for tbl in old.tables()}
        table_diffs = []
        for tbl in new.tables():
            old_locs = old_table_locations[tbl.name]
            new_locs = set(tbl.locations)

            additions = new_locs.difference(old_locs)
            removals = old_locs.difference(new_locs)

            if len(additions) > 0 or len(removals) > 0:
                table_diffs.append(TableDiff(tbl.name, list(additions), list(removals)))

        aurora_diff = ProvisioningDiff.of(
            old.aurora_provisioning(), new.aurora_provisioning()
        )
        redshift_diff = ProvisioningDiff.of(
            old.redshift_provisioning(), new.redshift_provisioning()
        )

        if len(table_diffs) == 0 and aurora_diff is None and redshift_diff is None:
            return None

        return cls(table_diffs, aurora_diff, redshift_diff)

    def __init__(
        self,
        table_diffs: List[TableDiff],
        aurora_diff: Optional[ProvisioningDiff],
        redshift_diff: Optional[ProvisioningDiff],
    ) -> None:
        self._table_diffs = table_diffs
        self._aurora_diff = aurora_diff
        self._redshift_diff = redshift_diff

    def aurora_diff(self) -> Optional[ProvisioningDiff]:
        return self._aurora_diff

    def redshift_diff(self) -> Optional[ProvisioningDiff]:
        return self._redshift_diff

    def table_diffs(self) -> List[TableDiff]:
        return self._table_diffs
