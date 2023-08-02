import pandas as pd
from typing import Iterator
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


def timestamp_iterator(
    window_start: datetime, window_end: datetime, delta: timedelta
) -> Iterator[datetime]:
    """
    Returns an iterator that emits all timestamps aligned to `delta` whose
    "forward windows" overlap with the given time window.

    NOTE: `window_start` is inclusive; `window_end` is exclusive.
    """
    assert window_start <= window_end
    aligned_start = window_start - (
        (window_start - datetime.min.replace(tzinfo=window_start.tzinfo)) % delta
    )

    curr = aligned_start
    while True:
        yield curr
        curr += delta
        if curr >= window_end:
            break


def windows_intersect(
    start1: datetime, end1: datetime, start2: datetime, end2: datetime
) -> bool:
    """
    Returns True iff the two windows intersect.
    NOTE: The left endpoint is inclusive. The right endpoint is exclusive.
    """
    return not (end1 <= start2 or end2 <= start1)


def time_point_intersect(start: datetime, end: datetime, timepoint: datetime) -> bool:
    """
    Returns True iff `timepoint` lies within [start, end).
    NOTE: The left endpoint is inclusive. The right endpoint is exclusive.
    """
    return timepoint >= start and timepoint < end
