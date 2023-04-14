from typing import Dict

from brad.blueprint import Blueprint
from brad.blueprint.user import UserProvidedBlueprint
from brad.blueprint.table import Table
from brad.config.engine import Engine


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
            table.locations.append(Engine.Aurora)

            # Other tables may depend on this table. So we also replicate it on
            # Redshift since we currently run transformations on Redshift.
            table.locations.append(Engine.Redshift)

            # If we reach this spot and this flag is true, then no other tables
            # are dependent on this table. In this case, we also replicate it across
            # Athena
            if expect_standalone_base_table:
                table.locations.append(Engine.Athena)

            is_base_table[table.name] = True
            return

        # This table *has* user-declared dependencies.
        # Recursively process dependents.
        for dep_tbl_name in table.table_dependencies:
            process_table(tables_by_name[dep_tbl_name], expect_standalone_base_table)

        # This table will be replicated on Redshift and S3.
        table.locations.append(Engine.Redshift)
        table.locations.append(Engine.Athena)

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

    # We pass through the provisioning hints provided by the user.
    return Blueprint(
        user.schema_name,
        list(tables_by_name.values()),
        user.aurora_provisioning(),
        user.redshift_provisioning(),
        None,
    )
