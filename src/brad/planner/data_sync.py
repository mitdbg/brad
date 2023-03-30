from typing import Dict, List

from brad.blueprint.data.blueprint import DataBlueprint
from brad.blueprint.data.table import Table, TableName
from brad.blueprint.data.location import Location
from brad.data_sync.logical_plan import (
    LogicalDataSyncPlan,
    LogicalDataSyncOperator,
    ExtractDeltas,
    TransformDeltas,
    ApplyDeltas,
)


def make_logical_data_sync_plan(blueprint: DataBlueprint) -> LogicalDataSyncPlan:
    # For each table, the operator whose output is the table's deltas.
    delta_operators: Dict[TableName, LogicalDataSyncOperator] = {}
    all_operators: List[LogicalDataSyncOperator] = []
    base_operators: List[LogicalDataSyncOperator] = []

    # Recursively traverse the table dependency graph and generate a logical
    # data sync plan.
    #
    # Key idea and overall strategy:
    # - We "sync" the tables by propagating the deltas from the base tables
    #   (tables without dependencies that have experienced writes) to all other
    #   tables in BRAD.
    # - This recursive function returns an operator that will produce deltas to
    #   apply to the given table.
    # - If the table is a base table, an "extract" operator will produce deltas
    #   for it.
    # - If the table has dependencies, we collect the delta operators from its
    #   dependencies and create a new "transform" operator that will transform
    #   the deltas into deltas to be applied to this table.
    # - While traversing the dependency graph, this function also generates
    #   "apply delta" operators to actually apply the changes to the table.
    def process_table(table: Table) -> LogicalDataSyncOperator:
        # Base case: Table already processed.
        if table.name in delta_operators:
            return delta_operators[table.name]

        # 1. Get the operator that will compute deltas to apply to this table.
        if len(table.table_dependencies) == 0:
            # This is a base table.
            extract_op = ExtractDeltas(table.name)
            all_operators.append(extract_op)
            base_operators.append(extract_op)
            delta_source_for_this_table: LogicalDataSyncOperator = extract_op

        else:
            # This table has dependencies. Recursively compute their delta
            # generator operators.
            source_delta_generators = list(
                map(
                    lambda dep_name: process_table(blueprint.get_table(dep_name)),
                    table.table_dependencies,
                )
            )

            # Sanity check.
            for gen in source_delta_generators:
                assert isinstance(gen, ExtractDeltas) or isinstance(
                    gen, TransformDeltas
                )

            if table.transform_text is None:
                # Identity transform. There must be only one source.
                assert len(source_delta_generators) == 0
                delta_source_for_this_table = source_delta_generators[0]

            else:
                # There is a transform.
                transform_op = TransformDeltas(
                    source_delta_generators,
                    table.transform_text,
                    # Initial heuristic: Run all transforms on Redshift. This
                    # can be made more sophisticated depending on system loads,
                    # the source data location, etc.
                    Location.Redshift,
                )
                all_operators.append(transform_op)
                delta_source_for_this_table = transform_op

        # 2. Create operators to apply deltas to this table (and its replicas).
        for location in table.locations:
            all_operators.append(
                ApplyDeltas(delta_source_for_this_table, table.name, location)
            )

        delta_operators[table.name] = delta_source_for_this_table
        return delta_source_for_this_table

    # Actually process the tables.
    for table in blueprint.tables:
        process_table(table)

    # Filter the operator list to remove operators with no dependencies and no
    # dependees. These are operators that would have been created for singular
    # tables with no dependencies and no replicas.
    all_operators = list(
        filter(
            lambda op: len(op.dependees()) > 0 or len(op.dependencies()) > 0,
            all_operators,
        )
    )
    base_operators = list(
        filter(
            lambda op: len(op.dependees()) > 0 or len(op.dependencies()) > 0,
            base_operators,
        )
    )

    return LogicalDataSyncPlan(all_operators, base_operators)
