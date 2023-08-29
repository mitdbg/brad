import argparse
from datetime import timedelta

from brad.daemon.perf_insights import PerfInsightsClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Retrieve metrics from Performance Insights for debug purposes."
    )
    parser.add_argument("--instance-id", type=str, required=True)
    parser.add_argument("--metric-name", type=str, required=True)
    parser.add_argument("--metric-stat", type=str, default="Average")
    parser.add_argument("--period-s", type=int, default=60)
    parser.add_argument("--num-entries", type=int, default=30)
    args = parser.parse_args()

    pi = PerfInsightsClient.from_instance_identifier(args.instance_id)
    df = pi.fetch_metrics(
        [(args.metric_name, args.metric_stat)],
        timedelta(seconds=args.period_s),
        args.num_entries,
    )
    print(df)
