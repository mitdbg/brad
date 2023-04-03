from brad.blueprint.data.table import Table


def source_table_name(table: str | Table) -> str:
    if isinstance(table, Table):
        str_name = table.name.value
    else:
        str_name = table
    return "{}_brad_source".format(str_name)


def shadow_table_name(table: str | Table) -> str:
    if isinstance(table, Table):
        str_name = table.name.value
    else:
        str_name = table
    return "{}_brad_shadow".format(str_name)


def delete_trigger_function_name(table: Table) -> str:
    return "{}_brad_delete_trigger_fn".format(table.name.value)


def delete_trigger_name(table: Table) -> str:
    return "{}_brad_delete_trigger".format(table.name.value)


def update_trigger_function_name(table: Table) -> str:
    return "{}_brad_update_trigger_fn".format(table.name.value)


def update_trigger_name(table: Table) -> str:
    return "{}_brad_update_trigger".format(table.name.value)


def seq_index_name(table: Table, for_shadow: bool) -> str:
    return "{}_brad_seq_index".format(
        table.name.value if not for_shadow else shadow_table_name(table)
    )


def imported_staging_table_name(table: Table) -> str:
    return "{}_brad_staging".format(table.name.value)


def imported_shadow_staging_table_name(table: Table) -> str:
    return "{}_brad_shadow_staging".format(table.name.value)


def insert_delta_table_name(table: str | Table) -> str:
    if isinstance(table, Table):
        str_name = table.name.value
    else:
        str_name = table
    return "{}_inserts".format(str_name)


def delete_delta_table_name(table: str | Table) -> str:
    if isinstance(table, Table):
        str_name = table.name.value
    else:
        str_name = table
    return "{}_deletes".format(str_name)


AURORA_EXTRACT_PROGRESS_TABLE_NAME = "brad_extract_progress"
AURORA_SEQ_COLUMN = "brad_seq"
