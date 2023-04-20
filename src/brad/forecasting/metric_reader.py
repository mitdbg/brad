import boto3
from datetime import datetime, timedelta
from typing import List


class MetricReader:
    """
    Utility class used for accessing monitoring metrics for the deployed services.
    """

    def __init__(
        self,
        service: str,
        metric_name: str,
        stats: List[str],
        epoch_minutes: int,
        redshift_cluster_id: str = "",
    ):
        if epoch_minutes > 60 * 24:
            raise ValueError(
                f"epoch_minutes can be at most 1440 (one day), not {epoch_minutes}"
            )
        if service == "redshift":
            if redshift_cluster_id == "":
                raise ValueError(
                    "To read metrics from redshift, must specify redshift_cluster_id"
                )
            self.namespace = "AWS/Redshift"
        elif service == "aurora":
            self.namespace = "AWS/RDS"

        self.service = service
        self.metric_name = metric_name
        self.stats = stats
        self.epoch_minutes = epoch_minutes
        self.redshift_cluster_id = redshift_cluster_id
        self.client = boto3.client("cloudwatch")

    # Stats for the epochs between i (inclusive) and j (exclusive), for negative i and nonpositive j.
    # Epoch 0 is currently in progress, positive epochs are in the future.
    def get_stats(self, i: int = -1, j: int = 0):
        if i >= 0 or j > 0:
            raise ValueError(
                "Can only get stats for past epochs (negative values of i and nonpositive values of j)"
            )
        if i >= j:
            raise ValueError("Must have i < j")

        end = datetime.now()
        end_floor = (
            end
            - timedelta(
                hours=end.hour % (self.epoch_minutes / 60),
                minutes=end.minute % self.epoch_minutes,
                seconds=end.second,
                microseconds=end.microsecond,
            )
            - timedelta(minutes=self.epoch_minutes * -j)
        )
        start = end_floor - timedelta(minutes=self.epoch_minutes * (j - i))

        if self.service == "redshift":
            dimensions = [
                {"Name": "ClusterIdentifier", "Value": self.redshift_cluster_id},
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
            Statistics=self.stats,
        )

        points = response["Datapoints"]
        points.reverse()

        for ep, point in enumerate(points):
            point["EpochStart"] = point.pop("Timestamp")
            point["Service"] = self.service
            point["Metric"] = response["Label"]
            point["Epoch"] = i + ep

        return points


if __name__ == "__main__":
    mr = MetricReader(
        "redshift", "CPUUtilization", ["Average", "Maximum"], 60, "brad-redshift"
    )
    print(mr.get_stats())
    print(mr.get_stats(i=-2))

    mr2 = MetricReader("aurora", "CPUUtilization", ["Average"], 30)
    print(mr2.get_stats())
    print(mr2.get_stats(i=-2))
