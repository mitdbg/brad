import pandas as pd
from datetime import datetime, timedelta

from brad.utils.time_periods import period_start, impute_old_missing_metrics


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
