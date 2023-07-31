from typing import Tuple

# Metric name, stat
# Example: CPUUtilization, Average (CloudWatch)
# Example: os.cpuUtilization.total, avg (Performance Insights)
MetricDef = Tuple[str, str]
