import logging
import pandas as pd

logger = logging.getLogger(__name__)


class MetricsThresholds:
    def __init__(
        self,
        lo: float,
        hi: float,
        sustained_epochs: int = 1,
    ) -> None:
        self._lo = lo
        self._hi = hi
        self._sustained_epochs = sustained_epochs

    def exceeds_thresholds(self, metric_values: pd.Series, log_desc: str) -> bool:
        rel = metric_values[-self._sustained_epochs :]
        if len(rel) < self._sustained_epochs:
            # Not enough data.
            return False

        if (rel < self._lo).all():
            logger.info(
                "Triggering replan because %s (%f) is below %f.",
                log_desc,
                rel.iloc[-1],
                self._lo,
            )
            return True
        if (rel > self._hi).all():
            logger.info(
                "Triggering replan because %s (%f) is above %f.",
                log_desc,
                rel.iloc[-1],
                self._hi,
            )
            return True

        return False
