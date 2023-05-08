from brad.forecasting.constant_forecaster import ConstantForecaster
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def test_num_points():
    dataframe = pd.DataFrame(
        {"a": np.random.randint(1, 10, 10), "b": np.random.randint(1, 10, 10)},
        index=pd.date_range("2022-01-01", "2022-01-10", freq="D", normalize=True),
    )

    f = ConstantForecaster(dataframe, timedelta(days=1))

    assert f.num_points("a", -3) == []
    assert f.num_points("a", 0) == []
    assert f.num_points("a", 3) == (3 * list(dataframe.tail(1)["a"]))
    assert f.num_points("b", 4) == (4 * list(dataframe.tail(1)["b"]))


def test_until():
    dataframe = pd.DataFrame(
        {"a": np.random.randint(1, 10, 10), "b": np.random.randint(1, 10, 10)},
        index=pd.date_range("2022-01-01", "2022-01-10", freq="D", normalize=True),
    )

    f = ConstantForecaster(dataframe, timedelta(days=1))

    assert f.until("a", datetime(year=2022, month=1, day=8)) == []

    assert f.until("a", datetime(year=2022, month=1, day=10)) == []

    assert f.until("a", datetime(year=2022, month=1, day=14)) == (
        4 * list(dataframe.tail(1)["a"])
    )

    assert f.until("a", datetime(year=2022, month=1, day=16, hour=5)) == (
        6 * list(dataframe.tail(1)["a"])
    )

    assert f.until("b", datetime(year=2022, month=1, day=17, hour=4)) == (
        7 * list(dataframe.tail(1)["b"])
    )
