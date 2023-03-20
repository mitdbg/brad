from typing import Tuple
from brad.config.dbtype import DBType


class RunTimePrediction:
    # NOTE: This class will likely change based on the cost model implementation.

    def __init__(self, athena_ms: float, aurora_ms: float, redshift_ms: float):
        self._athena_ms = athena_ms
        self._aurora_ms = aurora_ms
        self._redshift_ms = redshift_ms

    def run_time_ms(self, db: DBType) -> float:
        if db == DBType.Athena:
            return self._athena_ms
        elif db == DBType.Aurora:
            return self._aurora_ms
        elif db == DBType.Redshift:
            return self._redshift_ms
        else:
            raise AssertionError("Unhandled database: " + str(db))

    def min_time_ms(self) -> Tuple[DBType, float]:
        if self._athena_ms <= self._aurora_ms and self._athena_ms <= self._redshift_ms:
            return (DBType.Athena, self._athena_ms)
        elif (
            self._aurora_ms <= self._athena_ms and self._aurora_ms <= self._redshift_ms
        ):
            return (DBType.Aurora, self._aurora_ms)
        else:
            return (DBType.Redshift, self._redshift_ms)
