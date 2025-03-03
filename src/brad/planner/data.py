import logging
from typing import Dict, List

from brad.blueprint import Blueprint
from brad.blueprint.user import UserProvidedBlueprint
from brad.blueprint.table import Table
from brad.config.engine import Engine
from brad.routing.abstract_policy import FullRoutingPolicy
from brad.routing.round_robin import RoundRobin

logger = logging.getLogger(__name__)


def bootstrap_blueprint(user: UserProvidedBlueprint) -> Blueprint:
    """
    Generates a blueprint from a user-provided blueprint. This function is used
    for bootstrapping the system (generating the first blueprint from
    user-provided table schemas and dependencies).

    Effectively, this function makes decisions as to where to place each table
    and whether to replicate them.

    NOTE: This function mutates the passed-in blueprint.
    """

    # NOTE: This code assumes that the given `UserProvidedBlueprint` is
    # "well-formed" (e.g., no circular dependencies).

    tables_by_name = {tbl.name: tbl for tbl in user.tables}
    table_locations: Dict[str, List[Engine]] = {tbl.name: [] for tbl in user.tables}

    # To start (we'll do something more sophisticated when we have workload
    # information available):
    # - If a table is dependent on other tables, the "base tables" (i.e., the
    #   tables in the transitive closure with no dependencies) will be placed on
    #   Aurora. All other tables will be replicated across Redshift and S3.
    # - If a table has no user-declared dependencies, we replicate it across all
    #   three engines.

    # The bool indicates whether or not the table is a base table.
    is_base_table: Dict[str, bool] = dict()

    def process_table(table: Table, expect_standalone_base_table: bool):
        if table.name in is_base_table:
            return

        if len(table.table_dependencies) == 0:
            # Writes implicitly always originate on Aurora, so we will always
            # put a base table on Aurora.
            table_locations[table.name].append(Engine.Aurora)

            # Other tables may depend on this table. So we also replicate it on
            # Redshift since we currently run transformations on Redshift.
            table_locations[table.name].append(Engine.Redshift)

            # If we reach this spot and this flag is true, then no other tables
            # are dependent on this table. In this case, we also replicate it across
            # Athena
            if expect_standalone_base_table:
                table_locations[table.name].append(Engine.Athena)

            is_base_table[table.name] = True
            return

        # This table *has* user-declared dependencies.
        # Recursively process dependents.
        for dep_tbl_name in table.table_dependencies:
            process_table(tables_by_name[dep_tbl_name], expect_standalone_base_table)

        # This table will be replicated on Redshift and S3.
        table_locations[table.name].append(Engine.Redshift)
        table_locations[table.name].append(Engine.Athena)

        is_base_table[table.name] = False

    # First pass: Process all tables that declare a dependency.
    for table in tables_by_name.values():
        if len(table.table_dependencies) == 0:
            continue
        process_table(table, expect_standalone_base_table=False)

    # Second pass: Process all remaining tables.
    for table in tables_by_name.values():
        if len(table.table_dependencies) > 0:
            continue
        process_table(table, expect_standalone_base_table=True)

    # Sanity check: Each table should be present on at least one engine.
    assert all(map(lambda locs: len(locs) > 0, table_locations.values()))

    # Overwrite the placements where requested.
    bootstrap_locations = user.bootstrap_locations()
    for table_name in tables_by_name.keys():
        if table_name not in bootstrap_locations:
            continue
        new_locations = list(bootstrap_locations[table_name])
        logger.info("Setting the locations of %s to %s", table_name, str(new_locations))
        table_locations[table_name] = new_locations

    # We pass through the provisioning hints provided by the user.
    return Blueprint(
        user.schema_name,
        list(tables_by_name.values()),
        table_locations,
        user.aurora_provisioning(),
        user.redshift_provisioning(),
        # TODO: Replace the default definite policy.
        FullRoutingPolicy(indefinite_policies=[], definite_policy=RoundRobin()),
    )
