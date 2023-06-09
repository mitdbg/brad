from collections import namedtuple
from brad.daemon.monitor import Monitor

Metrics = namedtuple("Metrics", ["redshift_cpu_avg", "aurora_cpu_avg"])


def fetch_metrics(monitor: Monitor, forecasted: bool = True) -> Metrics:
    if forecasted:
        metrics = monitor.read_k_upcoming(
            k=1, metric_ids=list(_RELEVANT_METRICS.values())
        )
    else:
        metrics = monitor.read_k_most_recent(
            k=1, metric_ids=list(_RELEVANT_METRICS.values())
        )

    if metrics.empty:
        return Metrics(1.0, 1.0)

    redshift_cpu = metrics[_RELEVANT_METRICS["redshift_cpu_avg"]].iloc[0]
    aurora_cpu = metrics[_RELEVANT_METRICS["aurora_cpu_avg"]].iloc[0]

    return Metrics(redshift_cpu, aurora_cpu)


_RELEVANT_METRICS = {
    "redshift_cpu_avg": "redshift_CPUUtilization_Average",
    "aurora_cpu_avg": "aurora_WRITER_CPUUtilization_Average",
}
