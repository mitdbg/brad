import pandas as pd
from brad.planner.triggers.metrics_thresholds import MetricsThresholds


def test_metrics_thresholds():
    mtt = MetricsThresholds(lo=20, hi=80, sustained_epochs=3)

    low = pd.Series([10.0, 10.0, 19.0])
    low_not_sustained = pd.Series([10.0, 10.0, 29.0])
    assert mtt.exceeds_thresholds(low, "")
    assert not mtt.exceeds_thresholds(low_not_sustained, "")

    high = pd.Series([81, 85, 82])
    high_not_sustained = pd.Series([81, 75, 82])
    assert mtt.exceeds_thresholds(high, "")
    assert not mtt.exceeds_thresholds(high_not_sustained, "")
