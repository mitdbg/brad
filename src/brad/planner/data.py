from typing import List, Dict

from brad.blueprint.data import DataBlueprint
from brad.blueprint.data.location import Location
from brad.blueprint.data.user import UserProvidedDataBlueprint
from brad.blueprint.data.table import TableLocation, TableDependency, UserProvidedTable


def bootstrap_data_blueprint(user: UserProvidedDataBlueprint) -> DataBlueprint:
    """
    Generates a data blueprint from a user-provided blueprint. This function is
    used for bootstrapping the system (generating the first data blueprint from
    user-provided table schemas and dependencies).
    """

    # NOTE: This code assumes that the given `UserProvidedDataBlueprint` is
    # "well-formed" (e.g., no circular dependencies).

    user_tables_by_name = {tbl.name: tbl for tbl in user.tables}
    table_schemas = list(map(lambda tbl: tbl.as_schema(), user.tables))
    locations: List[TableLocation] = []
    dependencies: List[TableDependency] = []

    # To start (we'll do something more sophisticated when we have workload
    # information available):
    # - If a table is dependent on other tables, the "base tables" (i.e., the
    #   tables in the transitive closure with no dependencies) will be placed on
    #   Aurora. All other tables will be replicated across Redshift and S3.
    # - If a table has no user-declared dependencies, we replicate it across all
    #   three engines.

    # The bool indicates whether or not the table is a base table.
    is_base_table: Dict[str, bool] = dict()

    def process_user_table(
        user_table: UserProvidedTable, expect_standalone_base_table: bool
    ):
        if user_table.name in is_base_table:
            return

        if len(user_table.table_dependencies) == 0:
            # Writes implicitly always originate on Aurora, so we will always
            # put a base table on Aurora.
            aurora_tbl = TableLocation(user_table.name, Location.Aurora)
            locations.append(aurora_tbl)

            # If we reach this spot and this flag is true, then no other tables
            # are dependent on this table. In this case, we replicate it across
            # the other two engines.
            if expect_standalone_base_table:
                redshift_tbl = TableLocation(user_table.name, Location.Redshift)
                s3_tbl = TableLocation(user_table.name, Location.S3Iceberg)
                locations.append(redshift_tbl)
                locations.append(s3_tbl)
                dependencies.append(
                    TableDependency(
                        sources=[aurora_tbl], target=redshift_tbl, transform=None
                    )
                )
                dependencies.append(
                    TableDependency(sources=[aurora_tbl], target=s3_tbl, transform=None)
                )

            is_base_table[user_table.name] = True
            return

        # This table *has* user-declared dependencies.
        # Recursively process dependents.
        for dep_tbl_name in user_table.table_dependencies:
            process_user_table(
                user_tables_by_name[dep_tbl_name], expect_standalone_base_table
            )

        # This table will be replicated on Redshift and S3.
        redshift_tbl = TableLocation(user_table.name, Location.Redshift)
        s3_tbl = TableLocation(user_table.name, Location.S3Iceberg)

        # Add dependencies.
        # Heuristic: If the dependency is not a base table (e.g., a multi-hop
        # dependency chain), we take a dependency on the replica located in the
        # same place.
        dependencies.append(
            TableDependency(
                sources=list(
                    map(
                        lambda dep_name: TableLocation(dep_name, Location.Aurora)
                        if is_base_table[dep_name]
                        else TableLocation(dep_name, Location.Redshift),
                        user_table.table_dependencies,
                    )
                ),
                target=redshift_tbl,
                transform=user_table.transform_text,
            )
        )
        dependencies.append(
            TableDependency(
                sources=list(
                    map(
                        lambda dep_name: TableLocation(dep_name, Location.Aurora)
                        if is_base_table[dep_name]
                        else TableLocation(dep_name, Location.S3Iceberg),
                        user_table.table_dependencies,
                    )
                ),
                target=s3_tbl,
                transform=user_table.transform_text,
            )
        )

        is_base_table[user_table.name] = False

    # First pass: Process all tables that declare a dependency.
    for user_table in user.tables:
        if len(user_table.table_dependencies) == 0:
            continue
        process_user_table(user_table, expect_standalone_base_table=False)

    # Second pass: Process all remaining tables.
    for user_table in user.tables:
        if len(user_table.table_dependencies) > 0:
            continue
        process_user_table(user_table, expect_standalone_base_table=True)

    return DataBlueprint(user.db_name, table_schemas, locations, dependencies)
