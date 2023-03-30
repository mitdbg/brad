import boto3
import datetime


class MetricReader:
    """
    Utility class used for accessing monitoring metrics for the deployed services.
    """

    def __init__(self, service: str, metric_name: str):
        self.service = service
        self.metric_name = metric_name
        self.client = boto3.client("cloudwatch")
        if service == "redshift":
            self.namespace = "AWS/Redshift"
        elif service == "aurora":
            self.namespace = "AWS/RDS"

    def get_stats(self, minutes: int):
        now = datetime.datetime.now()
        now_floor = now - datetime.timedelta(
            seconds=now.second, microseconds=now.microsecond
        )
        yesterday = now_floor - datetime.timedelta(minutes=minutes)

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
            StartTime=yesterday,
            EndTime=now_floor,
            Period=minutes * 60,
            Statistics=[
                "SampleCount",
                "Average",
                "Minimum",
                "Maximum",
            ],
            Unit="Percent",
        )

        print(response["Datapoints"])


if __name__ == "__main__":
    mr = MetricReader("redshift", "CPUUtilization")
    mr.get_stats(60 * 24)
    mr.get_stats(60 * 24 * 2)

    mr2 = MetricReader("aurora", "CPUUtilization")
    mr2.get_stats(60 * 24)
    mr2.get_stats(60 * 24 * 2)
