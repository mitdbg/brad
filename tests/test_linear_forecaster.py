from brad.forecasting.linear_forecaster import LinearForecaster
import pandas as pd
from datetime import datetime, timedelta


def test_num_points():
    dataframe = pd.DataFrame(
        {"a": [i for i in range(10)], "b": [i for i in range(10, 20)]},
        index=pd.date_range("2022-01-01", "2022-01-10", freq="D", normalize=True),
    )

    f = LinearForecaster(dataframe, timedelta(days=1))

    assert f.num_points("a", -3) == []
    assert f.num_points("a", 0) == []
    assert f.num_points("a", 3) == [i for i in range(10, 13)]
    assert f.num_points("b", 4) == [i for i in range(20, 24)]


def test_until():
    dataframe = pd.DataFrame(
        {"a": [i for i in range(10)], "b": [i for i in range(10, 20)]},
        index=pd.date_range("2022-01-01", "2022-01-10", freq="D", normalize=True),
    )

    f = LinearForecaster(dataframe, timedelta(days=1))

    assert f.until("a", datetime(year=2022, month=1, day=8)) == []

    assert f.until("a", datetime(year=2022, month=1, day=10)) == []

    assert f.until("a", datetime(year=2022, month=1, day=14)) == [
        i for i in range(10, 14)
    ]

    assert f.until("a", datetime(year=2022, month=1, day=16, hour=5)) == [
        i for i in range(10, 16)
    ]

    assert f.until("b", datetime(year=2022, month=1, day=17, hour=4)) == [
        i for i in range(20, 27)
    ]
