from datetime import datetime, timedelta
from typing import List, Tuple

from brad.utils.streaming_metric import StreamingMetric


def get_value_stream(start: datetime) -> List[Tuple[float, datetime]]:
    return [
        (3.0, start),
        (10.0, start + timedelta(seconds=30)),
        (20.0, start + timedelta(seconds=60)),
    ]


def test_empty():
    start = datetime(year=2023, month=7, day=19)
    sm = StreamingMetric[float]()
    val = sm.average_since(start)
    assert val == 0.0


def test_multiple():
    start = datetime(year=2023, month=7, day=19)
    sm = StreamingMetric[float]()
    for val, ts in get_value_stream(start):
        sm.add_sample(val, ts)

    val = sm.average_since(start + timedelta(seconds=15))
    # Average of 10.0 and 20.0
    # We exclude the first datapoint because each sample covers the previous
    # time window.
    assert val == 15.0

    val = sm.average_since(start + timedelta(seconds=45))
    # Average of 20.0
    # We exclude the first two datapoints because each sample covers the
    # previous time window.
    assert val == 20.0


def test_all():
    start = datetime(year=2023, month=7, day=19)
    sm = StreamingMetric[float]()
    for val, ts in get_value_stream(start):
        sm.add_sample(val, ts)

    val = sm.average_since(start)
    # Average of 3.0, 10.0, and 20.0
    assert val == 11.0

    val = sm.average_since(start - timedelta(days=1))
    assert val == 11.0

    val = sm.average_since(start + timedelta(seconds=10))
    # We exclude the first datapoint because each sample covers the previous
    # time window.
    assert val == 15.0


def test_window_average():
    start = datetime(year=2023, month=7, day=26)
    sm = StreamingMetric[float]()
    sm.add_sample(3.0, start)
    sm.add_sample(10.0, start + timedelta(seconds=10))
    sm.add_sample(20.0, start + timedelta(seconds=20))
    sm.add_sample(30.0, start + timedelta(seconds=30))
    sm.add_sample(40.0, start + timedelta(seconds=40))

    # Window within one sample.
    val = sm.average_in_window(
        start + timedelta(seconds=1), start + timedelta(seconds=2)
    )
    assert val == 10.0

    # Window spanning two samples.
    val = sm.average_in_window(
        start + timedelta(seconds=1), start + timedelta(seconds=11)
    )
    assert val == 15.0

    # Window spanning three samples.
    val = sm.average_in_window(
        start + timedelta(seconds=1), start + timedelta(seconds=21)
    )
    assert val == (10.0 + 20.0 + 30.0) / 3

    # Window beyond end.
    val = sm.average_in_window(
        start + timedelta(seconds=1), start + timedelta(seconds=41)
    )
    assert val == (10.0 + 20.0 + 30.0 + 40.0) / 4

    # Whole range.
    val = sm.average_in_window(
        start - timedelta(seconds=1), start + timedelta(seconds=42)
    )
    assert val == (3.0 + 10.0 + 20.0 + 30.0 + 40.0) / 5

    # Before start.
    val = sm.average_in_window(
        start - timedelta(seconds=10), start - timedelta(seconds=1)
    )
    assert val == 3.0

    # After end.
    val = sm.average_in_window(
        start + timedelta(seconds=45), start + timedelta(seconds=50)
    )
    assert val == 0.0
