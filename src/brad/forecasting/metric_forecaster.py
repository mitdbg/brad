from brad.forecasting.metric_reader import MetricReader
from typing import List, Dict
from datetime import timedelta


class MetricForecaster:
    DEFAULT_REDSHIFT_METRICS = {
        "CommitQueueLength": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "ConcurrencyScalingActiveClusters": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "ConcurrencyScalingSeconds": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "CPUUtilization": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "DatabaseConnections": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "HealthStatus": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "MaintenanceMode": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "MaxConfiguredConcurrencyScalingClusters": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "NetworkReceiveThroughput": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "NetworkTransmitThroughput": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "PercentageDiskSpaceUsed": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "QueriesCompletedPerSecond": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "QueryDuration": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "QueryRuntimeBreakdown": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "ReadIOPS": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "ReadLatency": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "ReadThroughput": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "RedshiftManagedStorageTotalCapacity": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "TotalTableCount": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "WLMQueueLength": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "WLMQueueWaitTime": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "WLMQueriesCompletedPerSecond": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "WLMQueryDuration": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "WLMRunningQueries": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "WriteIOPS": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "WriteLatency": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "WriteThroughput": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "SchemaQuota": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "NumExceededSchemaQuotas": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "StorageUsed": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "PercentageQuotaUsed": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
    }

    DEFAULT_AURORA_METRICS = {
        "BufferCacheHitRatio": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "CommitLatency": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "CommitThroughput": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "CPUCreditBalance": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "CPUCreditUsage": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "CPUSurplusCreditBalance": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "CPUSurplusCreditsCharged": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "CPUUtilization": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "DatabaseConnections": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "DBLoad": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "DBLoadCPU": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "DBLoadNonCPU": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "Deadlocks": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "DiskQueueDepth": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "EBSByteBalance%": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "EBSIOBalance%": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "EngineUptime": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "FreeableMemory": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "FreeLocalStorage": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "MaximumUsedTransactionIDs": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "NetworkReceiveThroughput": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "NetworkThroughput": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "NetworkTransmitThroughput": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "OldestReplicationSlotLag": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "RDSToAuroraPostgreSQLReplicaLag": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "ReadIOPS": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "ReadLatency": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "ReadThroughput": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "ReplicationSlotDiskUsage": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "StorageNetworkReceiveThroughput": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "StorageNetworkThroughput": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "StorageNetworkTransmitThroughput": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "SwapUsage": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "TransactionLogsDiskUsage": [
            "SampleCount",
            "Average",
            "Sum",
            "Minimum",
            "Maximum",
        ],
        "WriteIOPS": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "WriteLatency": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
        "WriteThroughput": ["SampleCount", "Average", "Sum", "Minimum", "Maximum"],
    }

    def __init__(
        self,
        redshift_cluster_id: str,
        redshift_metrics: Dict[str, List[str]] | None = None,
        aurora_metrics: Dict[str, List[str]] | None = None,
        epoch_minutes=60 * 24,
    ):
        self.redshift_metrics = (
            redshift_metrics if redshift_metrics else self.DEFAULT_REDSHIFT_METRICS
        )
        self.aurora_metrics = (
            aurora_metrics if aurora_metrics else self.DEFAULT_AURORA_METRICS
        )
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
        if len(most_recent):
            most_recent[0]["EpochStart"] += timedelta(minutes=self.epoch_minutes) * (
                i + 1
            )
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

    fc2 = MetricForecaster(
        "brad-redshift",
        MetricForecaster.DEFAULT_REDSHIFT_METRICS,
        MetricForecaster.DEFAULT_AURORA_METRICS,
    )
    print(fc2.at_epoch(-1))
