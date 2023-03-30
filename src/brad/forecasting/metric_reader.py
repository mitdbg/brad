import boto3
from datetime import datetime, timedelta


class MetricReader:
    """
    Utility class used for accessing monitoring metrics for the deployed services.
    """

    def __init__(self, service: str, metric_name: str, stat: str, epoch_minutes: int):
        if epoch_minutes > 60 * 24:
            raise ValueError(
                f"epoch_minutes can be at most 1440 (one day), not {epoch_minutes}"
            )
        self.service = service
        self.metric_name = metric_name
        self.stat = stat
        self.epoch_minutes = epoch_minutes
        self.client = boto3.client("cloudwatch")
        if service == "redshift":
            self.namespace = "AWS/Redshift"
        elif service == "aurora":
            self.namespace = "AWS/RDS"

    # Stats for the i-th epoch, for negative i.
    # Epoch 0 is currently in progress, positive epochs are in the future.
    def get_stats(self, i: int = -1, end: datetime = datetime.now()):
        if i >= 0:
            raise ValueError(
                "Can only get stats for past epochs (negative values of i)"
            )

        end_floor = (
            end
            - timedelta(
                hours=end.hour % (self.epoch_minutes / 60),
                minutes=end.minute % self.epoch_minutes,
                seconds=end.second,
                microseconds=end.microsecond,
            )
            - timedelta(minutes=self.epoch_minutes * (-i - 1))
        )
        start = end_floor - timedelta(minutes=self.epoch_minutes)

        if self.service == "redshift":
            dimensions = [
                {"Name": "ClusterIdentifier", "Value": "brad-redshift"},
            ]
        elif self.service == "aurora":
            dimensions = [
                {"Name": "EngineName", "Value": "aurora-postgresql"},
            ]

        response = self.client.get_metric_statistics(
            Namespace=self.namespace,
            MetricName=self.metric_name,
            Dimensions=dimensions,
            StartTime=start,
            EndTime=end_floor,
            Period=self.epoch_minutes * 60,
            Statistics=[self.stat],
            Unit="Percent",
        )

        return response["Datapoints"][0][self.stat]


if __name__ == "__main__":
    mr = MetricReader("redshift", "CPUUtilization", "Average", 60)
    print(mr.get_stats())
    print(mr.get_stats(i=-2))
    print(mr.get_stats(end=datetime.now() - timedelta(days=1)))

    mr2 = MetricReader("aurora", "CPUUtilization", "Average", 30)
    print(mr2.get_stats())
    print(mr2.get_stats(i=-2))
    print(mr2.get_stats(end=datetime.now() - timedelta(days=1)))
