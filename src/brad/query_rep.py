import sqlglot
import sqlglot.expressions as exp

from typing import List, Optional

_DATA_MODIFICATION_PREFIXES = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
]


class QueryRep:
    """
    A SQL query's "internal representation" within BRAD.

    In practice, this class is used to abstract away the internal representation
    so that its implementation details are not part of the interface of other
    BRAD classes (e.g., we want to avoid exposing the query's parsed representation).
    """

    def __init__(self, sql_query: str):
        self._raw_sql_query = sql_query

        # Lazily computed.
        self._ast: Optional[sqlglot.Expression] = None
        self._is_data_modification: Optional[bool] = None
        self._tables: Optional[List[str]] = None

    @property
    def raw_query(self) -> str:
        return self._raw_sql_query

    def is_data_modification_query(self) -> bool:
        if self._is_data_modification is None:
            self._is_data_modification = any(
                map(self._raw_sql_query.upper().startswith, _DATA_MODIFICATION_PREFIXES)
            )
        return self._is_data_modification

    def is_transaction_start(self) -> bool:
        return self._raw_sql_query.upper() == "BEGIN"

    def is_transaction_end(self) -> bool:
        raw_sql = self._raw_sql_query.upper()
        return raw_sql == "COMMIT" or raw_sql == "ROLLBACK"

    def tables(self) -> List[str]:
        if self._tables is None:
            if self._ast is None:
                self._parse_query()
            assert self._ast is not None
            self._tables = list(
                map(lambda tbl: tbl.name, self._ast.find_all(exp.Table))
            )
        return self._tables

    def ast(self) -> sqlglot.Expression:
        if self._ast is None:
            self._parse_query()
        assert self._ast is not None
        return self._ast

    def _parse_query(self) -> None:
        self._ast = sqlglot.parse_one(self._raw_sql_query)
