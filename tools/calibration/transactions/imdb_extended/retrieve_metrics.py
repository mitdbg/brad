import argparse
from datetime import timedelta

from brad.daemon.perf_insights import AwsPerformanceInsightsClient


BASE_METRICS = [
    "os.loadAverageMinute.one",
    "os.loadAverageMinute.five",
    "os.loadAverageMinute.fifteen",
    "os.cpuUtilization.system",
    "os.cpuUtilization.total",
    "os.cpuUtilization.user",
    "os.diskIO.avgQueueLen",
    "os.diskIO.tps",
    "os.diskIO.util",
    "os.diskIO.readIOsPS",
    "os.diskIO.readKbPS",
    "os.diskIO.writeIOsPS",
    "os.diskIO.writeKbPS",
    "os.network.rx",
    "os.network.tx",
    "os.memory.active",
    "os.memory.dirty",
    "os.memory.free",
    "os.memory.writeback",
    "os.memory.total",
    "os.tasks.blocked",
    "os.tasks.running",
    "os.tasks.sleeping",
    "os.tasks.stopped",
    "os.tasks.total",
    "db.SQL.queries",
    "db.SQL.total_query_time",
    "db.SQL.tup_deleted",
    "db.SQL.tup_fetched",
    "db.SQL.tup_inserted",
    "db.SQL.tup_returned",
    "db.SQL.tup_updated",
    "db.Transactions.active_transactions",
    "db.Transactions.blocked_transactions",
    "db.Transactions.duration_commits",
    "db.Transactions.xact_commit",
    "db.Transactions.xact_rollback",
    # NOTE: Aurora has specific storage metrics (probably because they use a custom storage engine)
    # https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_PerfInsights_Counters.html#USER_PerfInsights_Counters.Aurora_PostgreSQL
    "os.diskIO.auroraStorage.auroraStorageBytesRx",
    "os.diskIO.auroraStorage.auroraStorageBytesTx",
    "os.diskIO.auroraStorage.diskQueueDepth",
    "os.diskIO.auroraStorage.readThroughput",
    "os.diskIO.auroraStorage.writeThroughput",
    "os.diskIO.auroraStorage.readLatency",
    "os.diskIO.auroraStorage.writeLatency",
    "os.diskIO.auroraStorage.readIOsPS",
    "os.diskIO.auroraStorage.writeIOsPS",
]

ALL_METRICS = []
for m in BASE_METRICS:
    # N.B. The metrics are reported no more than once a minute. So
    # average/max/min will all report the same number.
    ALL_METRICS.append(m + ".avg")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--instance-id",
        type=str,
        required=True,
        help="The Aurora instance's identifier.",
    )
    parser.add_argument(
        "--out-file",
        type=str,
        required=True,
        help="The path where the results should be saved.",
    )
    parser.add_argument(
        "--num-prev-points",
        type=int,
        default=60,
        help="The number of metric data points to retrieve.",
    )
    args = parser.parse_args()

    client = AwsPerformanceInsightsClient(args.instance_id)
    metrics = client.fetch_metrics(
        ALL_METRICS,
        period=timedelta(minutes=1),
        num_prev_points=args.num_prev_points,
    )
    metrics.to_csv(args.out_file)


if __name__ == "__main__":
    main()