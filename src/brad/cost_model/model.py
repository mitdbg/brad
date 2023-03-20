import math
from brad.config.dbtype import DBType
from brad.cost_model.prediction import RunTimePrediction

_DATA_MODIFICATION_PREFIXES = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
]

_AURORA_ONLY_PREDICTION = RunTimePrediction(
    aurora_ms=0,
    athena_ms=math.inf,
    redshift_ms=math.inf,
)


class CostModel:
    def predict_run_time(self, _sql_query: str) -> RunTimePrediction:
        raise NotImplementedError


class RoundRobinCostModel(CostModel):
    def __init__(self):
        self._next_db = DBType.Aurora

    def predict_run_time(self, sql_query: str) -> RunTimePrediction:
        # NOTE: Something more sophisticated goes here.

        # To start, we only run data modification statements on Aurora.
        if any(map(sql_query.startswith, _DATA_MODIFICATION_PREFIXES)):
            return _AURORA_ONLY_PREDICTION

        if self._next_db == DBType.Aurora:
            aurora_ms = float(len(sql_query))
            redshift_ms = aurora_ms * 2
            athena_ms = aurora_ms * 3
            self._next_db = DBType.Redshift

        elif self._next_db == DBType.Redshift:
            redshift_ms = float(len(sql_query))
            athena_ms = redshift_ms * 2
            aurora_ms = redshift_ms * 3
            self._next_db = DBType.Athena

        else:
            # DBType.Athena
            athena_ms = float(len(sql_query))
            aurora_ms = athena_ms * 2
            redshift_ms = athena_ms * 3
            self._next_db = DBType.Aurora

        return RunTimePrediction(
            athena_ms=athena_ms, aurora_ms=aurora_ms, redshift_ms=redshift_ms
        )
