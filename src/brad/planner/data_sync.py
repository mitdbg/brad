from typing import Dict, List, Optional

from brad.blueprint.data.blueprint import DataBlueprint
from brad.blueprint.data.table import Table
from brad.config.dbtype import DBType
from brad.data_sync.logical_plan import (
    LogicalDataSyncPlan,
    LogicalDataSyncOperator,
    ExtractDeltas,
    TransformDeltas,
    ApplyDeltas,
)


def make_logical_data_sync_plan(blueprint: DataBlueprint) -> LogicalDataSyncPlan:
    # For each table, the operator whose output is the table's deltas.
    delta_operators: Dict[str, Optional[LogicalDataSyncOperator]] = {}
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
    def process_table(table: Table) -> Optional[LogicalDataSyncOperator]:
        # Base case: Table already processed.
        if table.name in delta_operators:
            return delta_operators[table.name]

        # 1. Get the operator that will compute deltas to apply to this table.
        if len(table.table_dependencies) == 0:
            # This is a base table. If it has a replica on Aurora, we create an
            # `ExtractDeltas` op.
            if DBType.Aurora in table.locations:
                extract_op = ExtractDeltas(table.name)
                all_operators.append(extract_op)
                base_operators.append(extract_op)
                delta_source_for_this_table: Optional[
                    LogicalDataSyncOperator
                ] = extract_op
            else:
                # This scenario occurs when we have "static" table replicas
                # (e.g, a table replicated on Redshift and S3 that does not
                # experience writes).
                delta_source_for_this_table = None

        else:
            # This table has dependencies. Recursively compute their delta
            # generator operators.
            source_delta_generators = list(
                map(
                    lambda dep_name: process_table(blueprint.get_table(dep_name)),
                    table.table_dependencies,
                )
            )

            # Sanity check. All sources should not be `None` (otherwise it means
            # we are depending on a table that cannot be extracted from - a data
            # planning error).
            non_null_delta_sources: List[LogicalDataSyncOperator] = []
            for gen in source_delta_generators:
                assert gen is not None
                assert isinstance(gen, ExtractDeltas) or isinstance(
                    gen, TransformDeltas
                )
                non_null_delta_sources.append(gen)

            if table.transform_text is None:
                # Identity transform. There must be only one source.
                assert len(non_null_delta_sources) == 1
                delta_source_for_this_table = non_null_delta_sources[0]

            else:
                # There is a transform.
                transform_op = TransformDeltas(
                    non_null_delta_sources,
                    table.transform_text,
                    table.name,
                    # Initial heuristic: Run all transforms on Redshift. This
                    # can be made more sophisticated depending on system loads,
                    # the source data location, etc.
                    DBType.Redshift,
                )
                all_operators.append(transform_op)
                delta_source_for_this_table = transform_op

        # 2. Create operators to apply deltas to this table (and its replicas).
        # If this table is a base table and it does not have a replica on
        # Aurora, it is considered static (we assume writes originate on
        # Aurora).
        is_base_and_static = (
            len(table.table_dependencies) == 0 and DBType.Aurora not in table.locations
        )
        if not is_base_and_static:
            for location in table.locations:
                if len(table.table_dependencies) == 0 and location == DBType.Aurora:
                    # This is a base table. Writes originate from Aurora, so we do
                    # not need to apply deltas to it.
                    continue
                assert delta_source_for_this_table is not None
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
