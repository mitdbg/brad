from .model import CostModel
from brad.config.dbtype import DBType
from brad.cost_model.prediction import RunTimePrediction


class AlwaysOneCostModel(CostModel):
    """
    This cost model always returns the lowest cost for the same database engine.
    This model is useful for testing and benchmarking purposes.
    """

    def __init__(self, db: DBType):
        self._the_db = db

        if self._the_db == DBType.Aurora:
            aurora_ms = 1.0
            redshift_ms = aurora_ms * 2
            athena_ms = aurora_ms * 3

        elif self._the_db == DBType.Redshift:
            redshift_ms = 1.0
            athena_ms = redshift_ms * 2
            aurora_ms = redshift_ms * 3
            self._next_db = DBType.Athena

        else:
            # DBType.Athena
            athena_ms = 1.0
            aurora_ms = athena_ms * 2
            redshift_ms = athena_ms * 3

        self._prediction = RunTimePrediction(
            athena_ms=athena_ms, aurora_ms=aurora_ms, redshift_ms=redshift_ms
        )

    def predict_run_time(self, _sql_query: str) -> RunTimePrediction:
        return self._prediction
