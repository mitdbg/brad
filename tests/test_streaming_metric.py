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
    val = sm.average_since(start + timedelta(seconds=45))
    # Average of 10.0 and 20.0
    assert val == 15.0


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
    assert val == 11.0
