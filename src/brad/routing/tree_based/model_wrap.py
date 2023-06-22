import numpy as np
import numpy.typing as npt
from typing import List
from sklearn.ensemble import RandomForestClassifier

from . import ENGINE_LABELS
from brad.config.engine import Engine
from brad.query_rep import QueryRep


class ModelWrap:
    """
    A thin wrapper over the underlying model, used to avoid exposing
    implementation details to the router and to simplify
    serialization/deserialization.
    """

    def __init__(self, table_order: List[str], model: RandomForestClassifier) -> None:
        self._table_order = table_order
        self._model = model

    def engine_for(self, query: QueryRep) -> List[Engine]:
        """
        Produces a ranking of the engines for the query. The first engine in the
        list is the most preferable, followed by the second, and so on.
        """
        features = self._featurize_query(query)
        features = np.expand_dims(features, axis=0)
        preds = self._model.predict_proba(features)
        preds = np.squeeze(preds)
        low_to_high = np.argsort(preds)
        return [ENGINE_LABELS[label] for label in reversed(low_to_high)]

    def _featurize_query(self, query: QueryRep) -> npt.NDArray:
        one_hot_table_presence = np.zeros(len(self._table_order))
        for table in query.tables():
            try:
                table_idx = self._table_order.index(table)
                one_hot_table_presence[table_idx] = 1
            except ValueError:
                pass
        return one_hot_table_presence
