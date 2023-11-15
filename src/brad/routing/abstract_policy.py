from typing import List, Optional

from brad.config.engine import Engine
from brad.planner.estimator import Estimator
from brad.query_rep import QueryRep
from brad.routing.context import RoutingContext


class AbstractRoutingPolicy:
    """
    Note that implementers must be serializable.
    """

    def name(self) -> str:
        raise NotImplementedError

    async def engine_for(
        self, query_rep: QueryRep, ctx: RoutingContext
    ) -> List[Engine]:
        """
        Produces a preference order for query routing (the first element in the
        list is the most preferred engine, and so on).

        NOTE: Implementers currently do not need to consider DML queries. BRAD
        routes all DML queries to Aurora before consulting the router. Thus the
        query passed to this method will always be a read-only query.

        You should override this method if the routing policy needs to depend on
        any asynchronous methods.
        """
        return self.engine_for_sync(query_rep, ctx)

    def engine_for_sync(self, query_rep: QueryRep, ctx: RoutingContext) -> List[Engine]:
        """
        Produces a preference order for query routing (the first element in the
        list is the most preferred engine, and so on).

        NOTE: Implementers currently do not need to consider DML queries. BRAD
        routes all DML queries to Aurora before consulting the router. Thus the
        query passed to this method will always be a read-only query.
        """
        raise NotImplementedError


class FullRoutingPolicy:
    """
    Captures a full routing policy for serialization purposes. Indefinite
    policies are allowed to return empty preference lists (indicating no routing
    decision).
    """

    def __init__(
        self,
        indefinite_policies: List[AbstractRoutingPolicy],
        definite_policy: AbstractRoutingPolicy,
    ) -> None:
        self.indefinite_policies = indefinite_policies
        self.definite_policy = definite_policy

    def __eq__(self, other: object):
        if not isinstance(other, FullRoutingPolicy):
            return False
        return (self.indefinite_policies == other.indefinite_policies) and (
            self.definite_policy == other.definite_policy
        )
