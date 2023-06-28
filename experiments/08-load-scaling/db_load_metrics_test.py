import boto3
import json
import pytz
from datetime import datetime, timedelta, timezone


def fetch_metrics_max(epoch_length: timedelta, num_epochs: int):
    client = boto3.client("cloudwatch")

    queries = []
    instance_metrics = [
        "DBLoad",
        "DBLoadNonCPU",
        "DBLoadCPU",
        "CommitThroughput",
        "CPUUtilization",
    ]
    for im in instance_metrics:
        queries.append(
            {
                "Id": f"instance_{im}",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/RDS",
                        "MetricName": im,
                        "Dimensions": [
                            {
                                "Name": "DBInstanceIdentifier",
                                "Value": "aurora-2-instance-1",
                            }
                        ],
                    },
                    "Period": int(epoch_length.total_seconds()),
                    "Stat": "Maximum",
                },
                "ReturnData": True,
            }
        )

    # Retrieve datapoints
    now = datetime.now(tz=timezone.utc)
    end_time = now - (now - datetime.min.replace(tzinfo=pytz.UTC)) % epoch_length

    # Retrieve more than 1 epoch, for robustness; If we retrieve once per
    # minute and things are logged every minute, small delays might cause
    # us to miss some points. Deduplication is performed later on.
    start_time = end_time - num_epochs * epoch_length

    response_cloudwatch = client.get_metric_data(
        MetricDataQueries=queries,
        StartTime=start_time,
        EndTime=end_time,
        ScanBy="TimestampAscending",
    )

    return response_cloudwatch["MetricDataResults"]


def main():
    epoch_length = timedelta(seconds=60)
    num_epochs = 30
    res = fetch_metrics_max(epoch_length, num_epochs)
    print(json.dumps(res, indent=2, default=str))


if __name__ == "__main__":
    main()
