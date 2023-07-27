from datetime import datetime, timedelta

from brad.utils.time_periods import period_start


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
