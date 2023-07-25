from collections import namedtuple
from datetime import timedelta

from brad.config.engine import Engine, EngineBitmapValues
from brad.planner.scoring.context import ScoringContext


def compute_single_athena_table_cost(table_name: str, ctx: ScoringContext) -> float:
    athena_table_storage_usd_per_mb_per_month = 0.0

    # We make a rough estimate of the table's size on the engine.
    # This is an overestimate since we use Parquet to store the data on S3,
    # which will compress the columns.
    #
    # N.B. We use sizing information from the next workload here.
    num_rows = ctx.next_workload.table_num_rows(table_name)
    raw_extract_bytes = num_rows * ctx.planner_config.extract_table_bytes_per_row(
        ctx.schema_name, table_name
    )
    raw_extract_mb = raw_extract_bytes / 1000 / 1000

    athena_table_storage_usd_per_mb_per_month += (
        raw_extract_mb * ctx.planner_config.s3_usd_per_mb_per_month()
    )
    source_period = timedelta(days=30)
    dest_period = ctx.next_workload.period()

    return (
        athena_table_storage_usd_per_mb_per_month
        * (1.0 / source_period.total_seconds())
        * dest_period.total_seconds()
    )


TableMovementScore = namedtuple(
    "TableMovementScore", ["movement_cost", "movement_time_s"]
)


def compute_single_table_movement_time_and_cost(
    table_name: str,
    current_placement: int,
    next_placement: int,
    ctx: ScoringContext,
) -> TableMovementScore:
    movement_cost = 0.0
    movement_time_s = 0.0

    # Tables not currently present on an engine but will be present on an
    # engine (additions).
    added = (~current_placement) & next_placement

    if added == 0:
        # This means that this table is not being added to any engines.
        # Dropping a table is "free".
        return TableMovementScore(movement_cost, movement_time_s)

    added_engines = Engine.from_bitmap(added)

    move_from = _best_extract_engine(current_placement, ctx)
    extract_s3_rows = ctx.current_workload.table_num_rows(table_name)
    extract_s3_bytes = extract_s3_rows * ctx.planner_config.extract_table_bytes_per_row(
        ctx.schema_name, table_name
    )
    # N.B. The extract rates are all in MB/s, not MiB/s.
    extract_s3_mb = extract_s3_bytes / 1000 / 1000

    # Extraction scoring.
    if move_from == Engine.Athena:
        # N.B. "Extracting" data from Athena will depend on whether or not the
        # downstream engine(s) can read Iceberg Parquet files directly.
        # Otherwise, we will need to convert the data into a common format
        # (e.g., CSV).
        movement_time_s += (
            extract_s3_mb / ctx.planner_config.athena_extract_rate_mb_per_s()
        )
        movement_cost += ctx.planner_config.athena_usd_per_mb_scanned() * extract_s3_mb

    elif move_from == Engine.Aurora:
        movement_time_s += (
            extract_s3_mb / ctx.planner_config.aurora_extract_rate_mb_per_s()
        )

    elif move_from == Engine.Redshift:
        movement_time_s += (
            extract_s3_mb / ctx.planner_config.redshift_extract_rate_mb_per_s()
        )

    # Account for the computation needed to "import" data.
    for into_loc in added_engines:
        if into_loc == Engine.Athena:
            movement_time_s += (
                extract_s3_mb / ctx.planner_config.athena_load_rate_mb_per_s()
            )
            movement_cost += (
                ctx.planner_config.athena_usd_per_mb_scanned() * extract_s3_mb
            )

        elif into_loc == Engine.Aurora:
            movement_time_s += (
                extract_s3_mb / ctx.planner_config.aurora_load_rate_mb_per_s()
            )

        elif into_loc == Engine.Redshift:
            movement_time_s += (
                extract_s3_mb / ctx.planner_config.redshift_load_rate_mb_per_s()
            )

    return TableMovementScore(movement_cost, movement_time_s)


def _best_extract_engine(existing_locations: int, ctx: ScoringContext) -> Engine:
    """
    Returns the best source engine to extract a table from.
    """
    options = []

    if (existing_locations & EngineBitmapValues[Engine.Aurora]) != 0:
        options.append(
            (Engine.Aurora, ctx.planner_config.aurora_extract_rate_mb_per_s())
        )

    elif (existing_locations & EngineBitmapValues[Engine.Athena]) != 0:
        options.append(
            (Engine.Athena, ctx.planner_config.athena_extract_rate_mb_per_s())
        )

    elif (existing_locations & EngineBitmapValues[Engine.Redshift]) != 0:
        options.append(
            (Engine.Redshift, ctx.planner_config.redshift_extract_rate_mb_per_s())
        )

    options.sort(key=lambda op: op[1])

    if len(options) > 1 and options[0][0] == Engine.Athena:
        # Avoid Athena if possible because we may need to pay for extraction.
        return options[1][0]
    else:
        return options[0][0]
