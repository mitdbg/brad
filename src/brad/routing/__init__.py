from brad.config.dbtype import DBType
from brad.query_rep import QueryRep


class Router:
    def engine_for(self, query: QueryRep) -> DBType:
        """
        Selects an engine for the provided SQL query.
        """

        raise NotImplementedError
