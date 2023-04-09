from typing import Dict

from brad.blueprint.data import DataBlueprint
from brad.blueprint.data.location import Location
from brad.blueprint.data.user import UserProvidedDataBlueprint
from brad.blueprint.data.table import Table


def bootstrap_data_blueprint(user: UserProvidedDataBlueprint) -> DataBlueprint:
    """
    Generates a data blueprint from a user-provided blueprint. This function is
    used for bootstrapping the system (generating the first data blueprint from
    user-provided table schemas and dependencies).

    Effectively, this function makes decisions as to where to place each table
    and whether to replicate them.

    NOTE: This function mutates the passed-in blueprint.
    """

    # NOTE: This code assumes that the given `UserProvidedDataBlueprint` is
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
            table.locations.append(Location.Aurora)

            # Other tables may depend on this table. So we also replicate it on
            # Redshift since we currently run transformations on Redshift.
            table.locations.append(Location.Redshift)

            # If we reach this spot and this flag is true, then no other tables
            # are dependent on this table. In this case, we also replicate it across
            # Athena
            if expect_standalone_base_table:
                table.locations.append(Location.S3Iceberg)

            is_base_table[table.name] = True
            return

        # This table *has* user-declared dependencies.
        # Recursively process dependents.
        for dep_tbl_name in table.table_dependencies:
            process_table(tables_by_name[dep_tbl_name], expect_standalone_base_table)

        # This table will be replicated on Redshift and S3.
        table.locations.append(Location.Redshift)
        table.locations.append(Location.S3Iceberg)

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

    return DataBlueprint(user.schema_name, list(tables_by_name.values()))
