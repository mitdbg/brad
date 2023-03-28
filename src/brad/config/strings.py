from brad.blueprint.data.table import TableSchema


def shadow_table_name(table: TableSchema) -> str:
    return "{}_brad_shadow".format(table.name)


def delete_trigger_function_name(table: TableSchema) -> str:
    return "{}_brad_delete_trigger_fn".format(table.name)


def delete_trigger_name(table: TableSchema) -> str:
    return "{}_brad_delete_trigger".format(table.name)


def seq_index_name(table: TableSchema, for_shadow: bool) -> str:
    return "{}_brad_seq_index".format(
        table.name if not for_shadow else shadow_table_name(table)
    )


def imported_staging_table_name(table: TableSchema) -> str:
    return "{}_brad_staging".format(table.name)


def imported_shadow_staging_table_name(table: TableSchema) -> str:
    return "{}_brad_shadow_staging".format(table.name)


AURORA_EXTRACT_PROGRESS_TABLE_NAME = "brad_extract_progress"
AURORA_SEQ_COLUMN = "brad_seq"
