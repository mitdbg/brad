from brad.config.engine import Engine
from brad.query_rep import QueryRep


class Router:
    async def run_setup(self) -> None:
        """
        Should be called before using the router. This is used to set up any
        dynamic state.
        """

    def engine_for(self, query: QueryRep) -> Engine:
        """
        Selects an engine for the provided SQL query.
        """

        raise NotImplementedError
