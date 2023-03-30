import boto3
from datetime import datetime, timedelta


class MetricReader:
    """
    Utility class used for accessing monitoring metrics for the deployed services.
    """

    def __init__(self, service: str, metric_name: str, stat: str):
        self.service = service
        self.metric_name = metric_name
        self.stat = stat
        self.client = boto3.client("cloudwatch")
        if service == "redshift":
            self.namespace = "AWS/Redshift"
        elif service == "aurora":
            self.namespace = "AWS/RDS"

    def get_stats(self, minutes: int, end: datetime = datetime.now()):
        end_floor = end - timedelta(seconds=end.second, microseconds=end.microsecond)
        start = end_floor - timedelta(minutes=minutes)

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
            Period=minutes * 60,
            Statistics=[
                self.stat
            ],
            Unit="Percent",
        )

        print(response["Datapoints"])


if __name__ == "__main__":
    mr = MetricReader("redshift", "CPUUtilization", "Average")
    mr.get_stats(60 * 24)
    mr.get_stats(60 * 24 * 2)

    mr2 = MetricReader("aurora", "CPUUtilization", "Average")
    mr2.get_stats(60 * 24)
    mr2.get_stats(60 * 24 * 2)
    mr2.get_stats(60 * 24, end=datetime.now() - timedelta(days=1))
