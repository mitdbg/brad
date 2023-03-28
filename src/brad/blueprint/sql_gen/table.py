from brad.blueprint.data.table import Column
from brad.config.dbtype import DBType

from typing import List


def comma_separated_column_names(cols: List[Column]) -> str:
    return ", ".join(map(lambda c: c.name, cols))


def comma_separated_column_names_and_types(cols: List[Column], for_db: DBType) -> str:
    return ", ".join(
        map(
            lambda c: "{} {}".format(c.name, _type_for(c.data_type, for_db)),
            cols,
        )
    )


def _type_for(data_type: str, for_db: DBType) -> str:
    # A hacky way to ensure we use a supported type in each DBMS (Athena does
    # not support `TEXT` data).
    if data_type.upper() == "TEXT" and for_db == DBType.Athena:
        return "STRING"
    else:
        return data_type
