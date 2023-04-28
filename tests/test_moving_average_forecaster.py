from brad.forecasting.moving_average_forecaster import MovingAverageForecaster
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def test_num_points():
    dataframe = pd.DataFrame(
        {"a": [i for i in range(10)], "b": [i for i in range(10,20)]},
        index=pd.date_range("2022-01-01", "2022-01-10", freq="D", normalize=True),
    )

    f = MovingAverageForecaster(dataframe, timedelta(days=1))

    assert f.num_points("a", -3) == []
    assert f.num_points("a", 0) == []
    assert f.num_points("a", 3) == [7 for _ in range(3)]
    assert f.num_points("b", 4) == [17 for _ in range(4)]


def test_until():
    dataframe = pd.DataFrame(
        {"a": [i for i in range(10)], "b": [i for i in range(10,20)]},
        index=pd.date_range("2022-01-01", "2022-01-10", freq="D", normalize=True),
    )

    f = MovingAverageForecaster(dataframe, timedelta(days=1))

    assert f.until("a", datetime(year=2022, month=1, day=8)) == []

    assert f.until("a", datetime(year=2022, month=1, day=10)) == []

    assert f.until("a", datetime(year=2022, month=1, day=14)) == [7 for _ in range(4)]

    assert f.until("a", datetime(year=2022, month=1, day=16, hour=5)) == [7 for _ in range(6)]

    assert f.until("b", datetime(year=2022, month=1, day=17, hour=4)) == [17 for _ in range(7)]
