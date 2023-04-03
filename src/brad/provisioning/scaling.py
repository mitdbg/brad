import boto3
import datetime


# Handle scaling decisions for provisioning.
# Test all code paths.
class ProvisionScaling:
    # Create.
    def __init__(
        self,
        athena_catalog_name="bradcatalog0",
        redshift_cluster_name="brad-cluster0",
        aurora_cluster_name="brad-cluster0",
    ):
        self.cw = boto3.client("cloudwatch")
        self.athena_catalog_name = athena_catalog_name
        self.redshift_cluster_name = redshift_cluster_name
        self.aurora_cluster_name = aurora_cluster_name

    # Return the cost of athena.
    def get_athena_cost(self, window_minutes=10):
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(minutes=window_minutes)
        period = 60 * window_minutes
        response = self.cw.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "data_scanned",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/Athena",
                            "MetricName": "DataScanned",
                            "Dimensions": [
                                {
                                    "Name": "DataCatalog",
                                    "Value": self.athena_catalog_name,
                                }
                            ],
                        },
                        "Period": period,
                        "Stat": "Sum",
                    },
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
        )

        data_scanned_bytes = response["MetricDataResults"][0]["Values"][0]
        data_scanned_tb = data_scanned_bytes / (1024**4)  # Convert bytes to terabytes
        cost = data_scanned_tb / 5.0
        return cost

    # Return the hourly cost of redshift.
    def get_redshift_cost(self, node_type="ra3.xlplus", node_count=2):
        node_costs = {
            "dc2.large": 0.25,
            "dc2.8xlarge": 4.80,
            "ra3.xlplus": 1.086,
            "ra3.4xlarge": 3.26,
            "ra3.16xlarge": 13.04,
        }
        return node_costs[node_type] / node_count

    # Check if should boot redshift.
    # Will just check if the redshift cluster is cheaper than athena.
    def can_boot_redshift(
        self, node_type="ra3.xlplus", node_count=2, window_minutes=10
    ):
        athena_cost = self.get_athena_cost(window_minutes=window_minutes)
        redshift_cost = self.get_redshift_cost(
            node_type=node_type, node_count=node_count
        ) * (window_minutes / 60)
        return athena_cost > redshift_cost

    # Check redshift utilization.
    def check_redshift_utilization(
        self,
        check_overused=True,
        utilization_type="cpu",
        utilization_threshold=90,
        window_minutes=10,
    ):
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(minutes=window_minutes)
        period = 60 * window_minutes
        # Return metrics.
        metric_name_map = {
            "disk": "PercentageDiskSpaceUsed",
            "cpu": "CPUUtilization",
        }

        if utilization_type not in metric_name_map:
            raise ValueError(
                "Invalid utilization type. Valid types are 'disk', 'cpu', and 'memory'."
            )

        metric_name = metric_name_map[utilization_type]

        response = self.cw.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "redshift_utilization",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/Redshift",
                            "MetricName": metric_name,
                            "Dimensions": [
                                {
                                    "Name": "ClusterIdentifier",
                                    "Value": self.redshift_cluster_name,
                                },
                            ],
                        },
                        "Period": period,
                        "Stat": "Average",
                    },
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
        )

        utilization_value = response["MetricDataResults"][0]["Values"][0]
        # Check.
        if check_overused:
            return utilization_value > utilization_threshold
        else:
            return utilization_value < utilization_threshold

    # Get percentage of slow queries.
    def get_redshift_slow_percentage(
        self, running_time_threshold_secs=10, window_minutes=10
    ):
        running_time_threshold = running_time_threshold_secs * 1000
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(minutes=window_minutes)
        period = 60 * window_minutes
        # Retrieve metrics.
        response = self.cw.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "redshift_slow_query_percentage",
                    "Expression": f"100 * SUM(COUNT_IF(QueryDuration >= {running_time_threshold})) / SUM(COUNT(QueryDuration))",
                    "Label": "SlowQueryPercentage",
                },
                {
                    "Id": "query_duration_count",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/Redshift",
                            "MetricName": "QueryDuration",
                            "Dimensions": [
                                {
                                    "Name": "ClusterIdentifier",
                                    "Value": self.redshift_cluster_name,
                                }
                            ],
                        },
                        "Period": period,
                        "Stat": "Sum",
                    },
                    "ReturnData": False,
                },
            ],
            StartTime=start_time,
            EndTime=end_time,
        )

        slow_query_percentage = response["MetricDataResults"][0]["Values"][0]
        return slow_query_percentage

    # Check aurora utilization.
    def check_aurora_utilization(
        self,
        check_overused=True,
        utilization_type="memory",
        utilization_threshold=80,
        window_minutes=10,
        node_type="db.r6g.large",
    ):
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(minutes=window_minutes)
        period = 60 * window_minutes
        # Get metrics.
        metric_name_map = {
            "cpu": "CPUUtilization",
            "memory": "FreeableMemory",  # Note: This is freeable memory, not a percentage
        }
        #
        if utilization_type not in metric_name_map:
            raise ValueError(
                "Invalid utilization type. Valid types are 'disk', 'cpu', and 'memory'."
            )

        metric_name = metric_name_map[utilization_type]

        response = self.cw.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "aurora_utilization",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/RDS",
                            "MetricName": metric_name,
                            "Dimensions": [
                                {
                                    "Name": "DBClusterIdentifier",
                                    "Value": self.aurora_cluster_name,
                                },
                            ],
                        },
                        "Period": period,
                        "Stat": "Average",
                    },
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
        )

        utilization_value = response["MetricDataResults"][0]["Values"][0]
        if utilization_type == "memory":
            total_mem = {
                "db.r6g.large": 16,
                "db.r6g.xlarge": 32,
                "db.r6g.2xlarge": 64,
                "db.r6g.4xlarge": 128,
            }
            utilization_value /= total_mem[node_type]
        if check_overused:
            return utilization_value > utilization_threshold
        else:
            return utilization_value < utilization_threshold

    # Find if aurora slow.
    def is_aurora_slow(self, latency_threshold_ms=500, window_minutes=10):
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(minutes=window_minutes)
        period = 60 * window_minutes

        # Retrieve metrics.
        response = self.cw.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "select_latency",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/RDS",
                            "MetricName": "SelectLatency",
                            "Dimensions": [
                                {
                                    "Name": "DBInstanceIdentifier",
                                    "Value": self.aurora_cluster_name,
                                }
                            ],
                        },
                        "Period": period,
                        "Stat": "Average",
                        "Unit": "Milliseconds",
                    },
                },
                {
                    "Id": "dml_latency",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/RDS",
                            "MetricName": "DMLLatency",
                            "Dimensions": [
                                {
                                    "Name": "DBInstanceIdentifier",
                                    "Value": self.aurora_cluster_name,
                                }
                            ],
                        },
                        "Period": period,
                        "Stat": "Average",
                        "Unit": "Milliseconds",
                    },
                },
            ],
            StartTime=start_time,
            EndTime=end_time,
        )

        select_latency_count = response["MetricDataResults"][0]["Values"][0]
        dml_latency_count = response["MetricDataResults"][1]["Values"][0]
        latency = (select_latency_count + dml_latency_count) / 2
        return latency > latency_threshold_ms
