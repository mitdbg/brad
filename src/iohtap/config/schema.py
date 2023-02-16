import yaml

from typing import List


class Column:
    def __init__(self, name: str, data_type: str, is_primary: bool):
        self._name = name
        self._data_type = data_type
        self._is_primary = is_primary

    @property
    def name(self):
        return self._name

    @property
    def data_type(self):
        return self._data_type

    @property
    def is_primary(self):
        return self._is_primary

    @staticmethod
    def comma_separated_names(cols: List["Column"]) -> str:
        return ", ".join(map(lambda c: c.name, cols))

    @staticmethod
    def comma_separated_names_and_types(cols: List["Column"]) -> str:
        return ", ".join(map(lambda c: "{} {}".format(c.name, c.data_type), cols))


class Table:
    def __init__(self, name: str, columns: List[Column]):
        self._name = name
        self._columns = columns
        self._primary_key = list(filter(lambda c: c.is_primary, columns))

    @property
    def name(self) -> str:
        return self._name

    @property
    def columns(self) -> List[Column]:
        return self._columns

    @property
    def primary_key(self) -> List[Column]:
        return self._primary_key


class Schema:
    @classmethod
    def load(cls, path: str):
        with open(path, "r", encoding="UTF-8") as file:
            raw_yaml = yaml.load(file, Loader=yaml.Loader)

        tables: List[Table] = []
        for raw_table in raw_yaml:
            table_name = raw_table["table_name"]
            columns = list(map(Schema._parse_column, raw_table["columns"]))
            tables.append(Table(table_name, columns))

        return cls(tables)

    def __init__(self, tables: List[Table]):
        self._tables = tables

    @property
    def tables(self) -> List[Table]:
        return self._tables

    @staticmethod
    def _parse_column(raw_column) -> Column:
        return Column(
            raw_column["name"],
            raw_column["data_type"],
            "primary_key" in raw_column and raw_column["primary_key"],
        )
