import itertools
from typing import List, Iterator

from brad.blueprint.table import Table
from brad.config.engine import Engine


class TableLocationEnumerator:
    """
    Helps with enumerating possible table placements within some distance of a
    starting placement.

    The distance is designed to be arbitrarily defined, but is currently based
    on the number of table changes (where each table change is weighted
    equally).
    """

    def enumerate(
        self, base_placement: List[Table], max_num_actions: int
    ) -> Iterator[List[Table]]:
        """
        Enumerates table placements that are within `max_num_actions` table
        movements away from the starting placement.

        The placements emitted by this enumerator should not be "stored",
        because the physical objects are re-used during enumeration to avoid
        creating many short-lived objects. To build a new blueprint from an
        emitted provisioning, `clone()` the returned tables.
        """

        # Each table is either present on an engine or not. We represent the
        # tables as "switches", of which there are 3n of them (n tables). A
        # switch is "on" if the table is on the engine. We enumerate through all
        # possible switch changes up to `max_num_actions` changes (e.g.,
        # flipping at most 5 switches).

        num_engines = 3
        max_switches = len(base_placement) * num_engines
        assert max_num_actions <= max_switches

        base_locations = [
            _TableLocations.from_locations(tbl.locations) for tbl in base_placement
        ]
        iter_locations = [
            _TableLocations.from_locations(tbl.locations) for tbl in base_placement
        ]
        candidate_placement = [tbl.clone() for tbl in base_placement]

        # Special case: no changes.
        yield candidate_placement

        # Consider all possibilities: 1 switch change, 2 switch changes, ...,
        # `max_num_actions` switch changes
        for num_actions in range(1, max_num_actions + 1):
            for switches in itertools.combinations(range(max_switches), num_actions):
                # Make the switch changes.
                for switch in switches:
                    tbl_idx = switch // num_engines
                    switch_idx = switch % num_engines
                    iter_locations[tbl_idx].apply_switch(switch_idx)

                # Generate the placement.
                for placement, new_locations in zip(
                    candidate_placement, iter_locations
                ):
                    new_locations.into_locations(placement.locations)

                yield candidate_placement

                # Reset the switches.
                for base, new_locations in zip(base_locations, iter_locations):
                    new_locations.reset_to(base)


class _TableLocations:
    def __init__(self, on_aurora: bool, on_redshift: bool, on_athena: bool) -> None:
        self.on_aurora = on_aurora
        self.on_redshift = on_redshift
        self.on_athena = on_athena

    def __repr__(self) -> str:
        return "".join(
            [
                "_TableLocations(on_aurora=",
                str(self.on_aurora),
                ", on_redshift=",
                str(self.on_redshift),
                ", on_athena=",
                str(self.on_athena),
                ")",
            ]
        )

    def reset_to(self, loc: "_TableLocations") -> None:
        self.on_aurora = loc.on_aurora
        self.on_redshift = loc.on_redshift
        self.on_athena = loc.on_athena

    def apply_switch(self, idx: int) -> None:
        if idx == 0:
            self.on_aurora = not self.on_aurora
        elif idx == 1:
            self.on_redshift = not self.on_redshift
        elif idx == 2:
            self.on_athena = not self.on_athena
        else:
            raise ValueError("Index out of switch range: " + str(idx))

    @classmethod
    def from_locations(cls, locations: List[Engine]) -> "_TableLocations":
        on_aurora = Engine.Aurora in locations
        on_redshift = Engine.Redshift in locations
        on_athena = Engine.Athena in locations
        return cls(on_aurora, on_redshift, on_athena)

    def into_locations(self, dest: List[Engine]) -> None:
        dest.clear()
        if self.on_aurora:
            dest.append(Engine.Aurora)
        if self.on_redshift:
            dest.append(Engine.Redshift)
        if self.on_athena:
            dest.append(Engine.Athena)
