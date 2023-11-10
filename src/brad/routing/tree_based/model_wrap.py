import pickle
import numpy as np
import numpy.typing as npt
from typing import List, Optional
from sklearn.ensemble import RandomForestClassifier

from . import ENGINE_LABELS
from brad.config.engine import Engine
from brad.data_stats.estimator import Estimator
from brad.query_rep import QueryRep
from brad.routing.policy import RoutingPolicy


class ModelWrap:
    """
    A thin wrapper over the underlying model, used to avoid exposing
    implementation details to the router and to simplify
    serialization/deserialization.
    """

    @classmethod
    def from_pickle_bytes(cls, serialized: bytes) -> "ModelWrap":
        return pickle.loads(serialized)

    def __init__(
        self,
        policy: RoutingPolicy,
        table_order: List[str],
        model: RandomForestClassifier,
    ) -> None:
        self._policy = policy
        self._table_order = table_order
        self._model = model

    def policy(self) -> RoutingPolicy:
        return self._policy

    async def engine_for(
        self, query: QueryRep, estimator: Optional[Estimator]
    ) -> List[Engine]:
        """
        Produces a ranking of the engines for the query. The first engine in the
        list is the most preferable, followed by the second, and so on.
        """
        features = await self._featurize_query(query, estimator)
        features = np.expand_dims(features, axis=0)
        preds = self._model.predict_proba(features)
        preds = np.squeeze(preds)
        low_to_high = np.argsort(preds)
        return [ENGINE_LABELS[label] for label in reversed(low_to_high)]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ModelWrap):
            return False
        if self._policy != other._policy or self._table_order != other._table_order:
            return False

        if id(self._model) == id(other._model):
            return True

        # Not very ideal, but this will check for identical copies.
        serialized = pickle.dumps(self._model)
        other_serialized = pickle.dumps(other._model)
        return serialized == other_serialized

    def to_pickle(self) -> bytes:
        # TODO: Pickling might not be the best option.
        return pickle.dumps(self)

    async def _featurize_query(
        self, query: QueryRep, estimator: Optional[Estimator]
    ) -> npt.NDArray:
        if self._policy == RoutingPolicy.ForestTableSelectivity:
            assert estimator is not None
            table_selectivity = np.zeros(len(self._table_order))
            access_infos = await estimator.get_access_info(query)
            for ai in access_infos:
                tidx = self._table_order.index(ai.table_name)
                table_selectivity[tidx] = max(table_selectivity[tidx], ai.selectivity)
            return table_selectivity

        elif self._policy == RoutingPolicy.ForestTablePresence:
            one_hot_table_presence = np.zeros(len(self._table_order))
            for table in query.tables():
                try:
                    table_idx = self._table_order.index(table)
                    one_hot_table_presence[table_idx] = 1
                except ValueError:
                    pass
            return one_hot_table_presence

        else:
            assert False
