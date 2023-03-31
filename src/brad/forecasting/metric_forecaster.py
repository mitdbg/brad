from brad.forecasting.metric_reader import MetricReader
from typing import List, Dict
from datetime import timedelta


class MetricForecaster:
    def __init__(
        self,
        redshift_cluster_id: str,
        redshift_metrics: Dict[str, List[str]],
        aurora_metrics: Dict[str, List[str]],
        epoch_minutes=60 * 24,
    ):
        self.redshift_metrics = redshift_metrics
        self.aurora_metrics = aurora_metrics
        self.epoch_minutes = epoch_minutes

        self.readers = {}
        for metric in self.redshift_metrics.keys():
            name = "redshift_" + metric
            self.readers[name] = MetricReader(
                "redshift",
                metric,
                self.redshift_metrics[metric],
                epoch_minutes,
                redshift_cluster_id,
            )

        for metric in self.aurora_metrics.keys():
            name = "aurora_" + metric
            self.readers[name] = MetricReader(
                "aurora", metric, self.aurora_metrics[metric], epoch_minutes
            )

    # Stats for the i-th epoch. Epoch 0 is currently in progress, positive epochs are in the future.
    # Returns actual stats for negative values of i, forecasts for other values of i.
    def at_epoch(self, i: int):
        l = []
        for reader in self.readers.keys():
            l.extend(
                self.readers[reader].get_stats(i, i + 1)
                if i < 0
                else self.forecast_impl(reader, i)
            )

        return l

    # Values for a specific metric from start_epoch (inclusive) until end_epoch (exclusive)
    def by_metric(self, service: str, metric: str, start_epoch: int, end_epoch: int):
        if start_epoch > end_epoch:
            raise ValueError("start_epoch must be less than or equal to end_epoch")

        metric_name = f"{service}_{metric}"
        l = []
        if start_epoch < 0:
            l.extend(
                self.readers[metric_name].get_stats(start_epoch, min(end_epoch, 0))
            )
        for i in range(max(start_epoch, 0), max(end_epoch, 0)):
            l.extend(self.forecast(service, metric, i))
        return l

    # Forecast the value of a specific metric at epoch i.
    def forecast(self, service: str, metric: str, i: int):
        return self.forecast_impl(f"{service}_{metric}", i)

    def forecast_impl(self, metric_name: str, i: int):
        if i < 0:
            raise ValueError("Can only forecast values for future epochs.")
        most_recent = self.readers[metric_name].get_stats(-1, 0)
        most_recent[0]["EpochStart"] += timedelta(minutes=self.epoch_minutes) * (i + 1)
        most_recent[0]["Epoch"] = i
        return most_recent


if __name__ == "__main__":
    rm = {
        "CPUUtilization": ["Average", "Maximum"],
        "PercentageDiskSpaceUsed": ["Maximum"],
    }
    am = {"CPUUtilization": ["Average", "Maximum"]}

    fc = MetricForecaster("brad-redshift", rm, am)
    print(fc.at_epoch(-2))
    print(fc.at_epoch(-1))
    print(fc.at_epoch(0))
    print(fc.at_epoch(1))

    print(fc.by_metric("redshift", "CPUUtilization", -3, 2))
    print(fc.by_metric("redshift", "CPUUtilization", -2, 0))
    print(fc.by_metric("redshift", "CPUUtilization", 1, 3))
