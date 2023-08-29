import pandas as pd
from datetime import datetime, timedelta

from brad.utils.time_periods import (
    period_start,
    impute_old_missing_metrics,
    timestamp_iterator,
    time_point_intersect,
)


def test_period_start():
    ts = datetime(year=2023, month=7, day=27, hour=12, minute=15, second=5)

    start_hour = period_start(ts, timedelta(hours=1))
    expected_start_hour = datetime(year=2023, month=7, day=27, hour=12)
    assert start_hour == expected_start_hour

    start_minute = period_start(ts, timedelta(minutes=1))
    expected_start_minute = datetime(year=2023, month=7, day=27, hour=12, minute=15)
    assert start_minute == expected_start_minute

    start_day = period_start(ts, timedelta(days=1))
    expected_start_day = datetime(year=2023, month=7, day=27)
    assert start_day == expected_start_day


def test_impute():
    ts = datetime(year=2023, month=7, day=29, hour=12, minute=15)
    minute = timedelta(minutes=1)
    df = pd.DataFrame.from_records(
        [
            (ts, 1.0),
            (ts + minute, 2.0),
            (ts + 2 * minute, float("nan")),
        ],
        columns=["timestamp", "value"],
        index="timestamp",
    )
    df.index = pd.to_datetime(df.index, utc=True, unit="ns")

    ndf = impute_old_missing_metrics(df, ts + timedelta(seconds=30), value=0.0)
    ndf = ndf.dropna()
    assert len(ndf) == 2

    ndf2 = impute_old_missing_metrics(df, ts + timedelta(days=1), value=0.0)
    ndf2 = ndf2.dropna()
    assert len(ndf2) == 3
    assert ndf2["value"].iloc[-1] == 0.0

    ndf3 = impute_old_missing_metrics(df, ts + timedelta(days=1), value=42.0)
    ndf3 = ndf3.dropna()
    assert len(ndf3) == 3
    assert ndf3["value"].iloc[-1] == 42.0


def test_timestamp_iterator():
    window_start = datetime(year=2023, month=8, day=2, hour=12, minute=15)
    window_end = datetime(year=2023, month=8, day=2, hour=13, minute=16)
    delta = timedelta(hours=1)

    values = []
    for ts in timestamp_iterator(window_start, window_end, delta):
        values.append(ts)
    assert values == [
        datetime(year=2023, month=8, day=2, hour=12),
        datetime(year=2023, month=8, day=2, hour=13),
    ]

    window_end2 = datetime(year=2023, month=8, day=2, hour=13)
    values.clear()
    for ts in timestamp_iterator(window_start, window_end2, delta):
        values.append(ts)
    assert values == [
        datetime(year=2023, month=8, day=2, hour=12),
    ]

    window_end3 = datetime(year=2023, month=8, day=2, hour=15, minute=10)
    values.clear()
    for ts in timestamp_iterator(window_start, window_end3, delta):
        values.append(ts)
    assert values == [
        datetime(year=2023, month=8, day=2, hour=12),
        datetime(year=2023, month=8, day=2, hour=13),
        datetime(year=2023, month=8, day=2, hour=14),
        datetime(year=2023, month=8, day=2, hour=15),
    ]

    window_end4 = datetime(year=2023, month=8, day=2, hour=12, minute=20)
    values.clear()
    for ts in timestamp_iterator(window_start, window_end4, delta):
        values.append(ts)
    assert values == [
        datetime(year=2023, month=8, day=2, hour=12),
    ]

    window_start2 = datetime(year=2023, month=8, day=2, hour=12)
    values.clear()
    for ts in timestamp_iterator(window_start2, window_end3, delta):
        values.append(ts)
    assert values == [
        datetime(year=2023, month=8, day=2, hour=12),
        datetime(year=2023, month=8, day=2, hour=13),
        datetime(year=2023, month=8, day=2, hour=14),
        datetime(year=2023, month=8, day=2, hour=15),
    ]


def test_time_point_intersect():
    start = datetime(year=2023, month=8, day=2, hour=12)
    end = datetime(year=2023, month=8, day=2, hour=13)

    assert not time_point_intersect(start, end, datetime(year=2023, month=8, day=2))
    assert not time_point_intersect(
        start, end, datetime(year=2023, month=8, day=2, hour=13)
    )
    assert time_point_intersect(
        start, end, datetime(year=2023, month=8, day=2, hour=12)
    )
    assert time_point_intersect(
        start, end, datetime(year=2023, month=8, day=2, hour=12, minute=10)
    )
