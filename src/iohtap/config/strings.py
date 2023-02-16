from .schema import Table


def shadow_table_name(table: Table) -> str:
    return "{}_iohtap_shadow".format(table.name)


def delete_trigger_function_name(table: Table) -> str:
    return "{}_iohtap_delete_trigger_fn".format(table.name)


def delete_trigger_name(table: Table) -> str:
    return "{}_iohtap_delete_trigger".format(table.name)


def seq_index_name(table: Table, for_shadow: bool) -> str:
    return "{}_iohtap_seq_index".format(
        table.name if not for_shadow else shadow_table_name(table)
    )


def imported_staging_table_name(table: Table) -> str:
    return "{}_iohtap_staging".format(table.name)


def imported_shadow_staging_table_name(table: Table) -> str:
    return "{}_iohtap_shadow_staging".format(table.name)


AURORA_EXTRACT_PROGRESS_TABLE_NAME = "iohtap_extract_progress"
AURORA_SEQ_COLUMN = "iohtap_seq"
