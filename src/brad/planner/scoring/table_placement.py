from collections import namedtuple
from typing import Dict

from brad.config.engine import Engine, EngineBitmapValues
from brad.config.planner import PlannerConfig
from brad.planner.workload import Workload


def compute_athena_table_placement_cost(
    table_placements: Dict[str, int],
    workload: Workload,
    planner_config: PlannerConfig,
) -> float:
    """
    Estimates the hourly monetary cost of storing a table on Athena.
    """
    athena_table_storage_cost = 0.0
    sources = [Engine.Athena, Engine.Aurora, Engine.Redshift]
    for tbl, locations in table_placements.items():
        if locations & EngineBitmapValues[Engine.Athena] == 0:
            # This table is not present on Athena.
            continue

        # We make a rough estimate of the table's size on the engine.
        for src in sources:
            size_mb = workload.table_size_on_engine(tbl, src)
            if size_mb is not None:
                break

        # Table is present on at least one engine.
        assert size_mb is not None

        athena_table_storage_cost += size_mb * planner_config.s3_usd_per_mb_per_month()

    # Rescale the cost to be USD per MB per hour. The provisioning cost is
    # based on an hour.
    # We use 30 days to represent a month.
    # TODO: Make the time period configurable (we may want a cost for a day,
    # for example).
    athena_table_storage_cost /= 30 * 24

    return athena_table_storage_cost


TableMovementScore = namedtuple(
    "TableMovementScore", ["movement_cost", "movement_time_s"]
)


def compute_table_movement_time_and_cost(
    current_placement: Dict[str, int],
    next_placement: Dict[str, int],
    current_workload: Workload,
    planner_config: PlannerConfig,
) -> TableMovementScore:
    # Table movement.
    movement_cost = 0.0
    movement_time_s = 0.0

    # We currently do not handle schema changes (adding/removing tables).
    assert len(current_placement) == len(next_placement)

    for table_name, cur in current_placement.items():
        nxt = next_placement[table_name]

        # Tables not currently present on an engine but will be present on an
        # engine (additions).
        added = (~cur) & nxt

        if added == 0:
            # This means that this table is not being added to any engines.
            # Dropping a table is "free".
            continue

        added_engines = Engine.from_bitmap(added)

        move_from = _best_extract_engine(cur, planner_config)
        source_table_size_mb = current_workload.table_size_on_engine(
            table_name, move_from
        )
        assert source_table_size_mb is not None

        # Extraction scoring.
        if move_from == Engine.Athena:
            movement_time_s += (
                source_table_size_mb / planner_config.athena_extract_rate_mb_per_s()
            )
            movement_cost += (
                planner_config.athena_usd_per_mb_scanned() * source_table_size_mb
            )

        elif move_from == Engine.Aurora:
            movement_time_s += (
                source_table_size_mb / planner_config.aurora_extract_rate_mb_per_s()
            )

        elif move_from == Engine.Redshift:
            movement_time_s += (
                source_table_size_mb / planner_config.redshift_extract_rate_mb_per_s()
            )

        # Account for the computation needed to "import" data.
        for into_loc in added_engines:
            # Need to assume the table will have the same size as on the
            # source engine. This is not necessarily true when Redshift
            # is the source, because it uses compression.
            if into_loc == Engine.Athena:
                movement_time_s += (
                    source_table_size_mb / planner_config.athena_load_rate_mb_per_s()
                )
                movement_cost += (
                    planner_config.athena_usd_per_mb_scanned() * source_table_size_mb
                )

            elif into_loc == Engine.Aurora:
                movement_time_s += (
                    source_table_size_mb / planner_config.aurora_load_rate_mb_per_s()
                )

            elif into_loc == Engine.Redshift:
                movement_time_s += (
                    source_table_size_mb / planner_config.redshift_load_rate_mb_per_s()
                )

    return TableMovementScore(movement_cost, movement_time_s)


def _best_extract_engine(
    existing_locations: int, planner_config: PlannerConfig
) -> Engine:
    """
    Returns the best source engine to extract a table from.
    """
    options = []

    if (existing_locations & EngineBitmapValues[Engine.Aurora]) != 0:
        options.append((Engine.Aurora, planner_config.aurora_extract_rate_mb_per_s()))

    elif (existing_locations & EngineBitmapValues[Engine.Athena]) != 0:
        options.append((Engine.Athena, planner_config.athena_extract_rate_mb_per_s()))

    elif (existing_locations & EngineBitmapValues[Engine.Redshift]) != 0:
        options.append(
            (Engine.Redshift, planner_config.redshift_extract_rate_mb_per_s())
        )

    options.sort(key=lambda op: op[1])

    if len(options) > 1 and options[0][0] == Engine.Athena:
        # Avoid Athena if possible because we need to pay for extraction.
        return options[1][0]
    else:
        return options[0][0]
