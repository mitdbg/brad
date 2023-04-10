from brad.config.engine import Engine
from brad.query_rep import QueryRep


class Router:
    def engine_for(self, query: QueryRep) -> Engine:
        """
        Selects an engine for the provided SQL query.
        """

        raise NotImplementedError
