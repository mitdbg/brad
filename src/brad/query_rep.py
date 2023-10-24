import sqlglot
import sqlglot.expressions as exp
import yaml
import pathlib
import os
from brad.routing.functionality_catalog import Functionality
from brad.front_end.session import Session
from typing import List, Optional

_DATA_MODIFICATION_PREFIXES = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    # HACK: Used to set the session's isolation level on Aurora/PostgreSQL.
    # A better way is to provide a special debug API that will let us force
    # commands on specific engines.
    "SET SESSION",
    "TRUNCATE",
]

# Load geospatial keywords used to detect if geospatial query
_GEOSPATIAL_KEYWORDS_PATH = os.path.join(
    pathlib.Path(__file__).parent.resolve(), "routing/geospatial_keywords.yml"
)
with open(_GEOSPATIAL_KEYWORDS_PATH, "r") as f:
    _GEOSPATIAL_KEYWORDS = yaml.safe_load(f)
_GEOSPATIAL_KEYWORDS = [k.upper() for k in _GEOSPATIAL_KEYWORDS]


class QueryRep:
    """
    A SQL query's "internal representation" within BRAD.

    In practice, this class is used to abstract away the internal representation
    so that its implementation details are not part of the interface of other
    BRAD classes (e.g., we want to avoid exposing the query's parsed representation).

    Objects of this class are logically immutable.
    """

    def __init__(self, sql_query: str, session: Optional[Session] = None):
        self._raw_sql_query = sql_query

        # Lazily computed.
        self._ast: Optional[sqlglot.Expression] = None
        self._is_data_modification: Optional[bool] = None
        self._tables: Optional[List[str]] = None
        self.in_transaction: bool = False
        if session is not None:
            self.in_transaction = session.in_transaction

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

    def is_geospatial(self) -> bool:
        query = self._raw_sql_query.upper()
        for keyword in _GEOSPATIAL_KEYWORDS:
            if keyword in query:
                return True
        return False

    def is_transaction(self) -> bool:
        return self.is_data_modification_query() or self.in_transaction

    def get_required_functionality(self) -> int:
        req_functionality: List[str] = []
        if self.is_geospatial():
            req_functionality.append(Functionality.Geospatial)
        if self.is_transaction():
            req_functionality.append(Functionality.Transaction)

        return Functionality.to_bitmap(req_functionality)

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
