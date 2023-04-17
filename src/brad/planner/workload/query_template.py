from typing import List


class QueryTemplate:
    def __init__(self, tables: List[str], is_transactional: bool) -> None:
        self._tables = tables
        self._is_transactional = is_transactional

    def tables(self) -> List[str]:
        """
        The tables referenced by this query template.
        """
        return self._tables

    def is_transactional(self) -> bool:
        """
        Whether or not this query template appears inside a transaction.
        """
        return self._is_transactional
