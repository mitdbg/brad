from brad.forecasting.metric_reader import MetricReader
from typing import List, Dict


class MetricForecaster:
    def __init__(
        self,
        redshift_metrics: Dict[str, List[str]],
        aurora_metrics: Dict[str, List[str]],
        epoch_minutes=60 * 24,
    ):
        self.redshift_metrics = redshift_metrics
        self.aurora_metrics = aurora_metrics
        self.epoch_minutes = epoch_minutes

        self.readers = {}
        for metric in self.redshift_metrics.keys():
            for stat in self.redshift_metrics[metric]:
                name = "redshift_" + metric + "_" + stat
                self.readers[name] = MetricReader(
                    "redshift", metric, stat, epoch_minutes
                )

        for metric in self.aurora_metrics.keys():
            for stat in self.aurora_metrics[metric]:
                name = "aurora_" + metric + "_" + stat
                self.readers[name] = MetricReader("aurora", metric, stat, epoch_minutes)

    # Stats for the i-th epoch. Epoch 0 is currently in progress, positive epochs are in the future.
    # Returns actual stats for negative values of i, forecasts for other values of i.
    def at_epoch(self, i):
        d = {}
        for reader in self.readers.keys():
            value = (
                self.readers[reader].get_stats(i) if i < 0 else self.forecast(reader, i)
            )
            d[reader] = value

        return d

    # Values for a specific metric-stat pair from start_epoch (inclusive) until end_epoch (exclusive)
    def by_metric(self, metric_stat, start_epoch, end_epoch):
        if start_epoch > end_epoch:
            raise ValueError("start_epoch must be less than or equal to end_epoch")

        l = []
        i = start_epoch
        for i in range(start_epoch, end_epoch):
            l.append(
                self.readers[metric_stat].get_stats(i)
                if i < 0
                else self.forecast(metric_stat, i)
            )
        return l

    # Forecast the value of a specific metric-stat pair at epoch i.
    def forecast(self, metric_stat, i):
        if i < 0:
            raise ValueError("Can only forecast values for future epochs.")
        return self.readers[metric_stat].get_stats(-1)


if __name__ == "__main__":
    rm = {
        "CPUUtilization": ["Average", "Maximum"],
        "PercentageDiskSpaceUsed": ["Maximum"],
    }
    am = {"CPUUtilization": ["Average", "Maximum"]}

    fc = MetricForecaster(rm, am)
    print(fc.at_epoch(-2))
    print(fc.at_epoch(-1))
    print(fc.at_epoch(0))
    print(fc.at_epoch(1))

    print(fc.by_metric("redshift_CPUUtilization_Average", -3, 2))
    print(fc.by_metric("redshift_CPUUtilization_Average", -2, 0))
    print(fc.by_metric("redshift_CPUUtilization_Average", 1, 3))
