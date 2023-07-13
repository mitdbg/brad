from typing import Optional
from brad.data_stats.estimator import Estimator


class EstimatorProvider:
    def get_estimator(self) -> Optional[Estimator]:
        return None


class FixedEstimatorProvider(EstimatorProvider):
    def __init__(self, estimator: Estimator) -> None:
        self._estimator = estimator

    def get_estimator(self) -> Optional[Estimator]:
        return self._estimator
