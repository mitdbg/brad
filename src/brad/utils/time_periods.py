import pandas as pd
from datetime import datetime, timedelta


def period_start(timestamp: datetime, period_length: timedelta) -> datetime:
    """
    Returns the aligned period start timestamp associated with a given
    `timestamp`.
    """
    return (
        timestamp
        - (timestamp - datetime.min.replace(tzinfo=timestamp.tzinfo)) % period_length
    )


def impute_old_missing_metrics(
    metrics: pd.DataFrame, cutoff: datetime, value: float = 0.0
) -> pd.DataFrame:
    """
    Replaces any NaN metric values that have a timestamp older than `cutoff`.
    """
    cpy = metrics.copy()
    cpy.loc[cpy.index < cutoff] = cpy.loc[cpy.index < cutoff].fillna(value)
    return cpy
