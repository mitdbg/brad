import boto3
import datetime


class MetricReader:
    """
    Utility class used for accessing monitoring metrics for the deployed services.
    """

    def __init__(self, namespace: str, metric_name: str):
        self.namespace = namespace
        self.metric_name = metric_name
        self.client = boto3.client("cloudwatch")

    def get_stats(self, minutes: int):
        now = datetime.datetime.now()
        now_floor = now - datetime.timedelta(
            seconds=now.second, microseconds=now.microsecond
        )
        yesterday = now_floor - datetime.timedelta(minutes=minutes)

        response = self.client.get_metric_statistics(
            Namespace=self.namespace,
            MetricName=self.metric_name,
            Dimensions=[
                {"Name": "ClusterIdentifier", "Value": "brad-redshift"},
            ],
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
    mr = MetricReader("AWS/Redshift", "CPUUtilization")
    mr.get_stats(60 * 24)
    mr.get_stats(60 * 24 * 2)
