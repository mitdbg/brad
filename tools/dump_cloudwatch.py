import argparse
from datetime import timedelta

from brad.config.engine import Engine
from brad.daemon.cloudwatch import CloudWatchClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Retrieve metrics from CloudWatch for debug purposes."
    )
    parser.add_argument("--engine", type=str, required=True)
    parser.add_argument("--cluster-id", type=str, required=True)
    parser.add_argument("--metric-name", type=str, required=True)
    parser.add_argument("--metric-stat", type=str, default="Average")
    parser.add_argument("--period-s", type=int, default=60)
    parser.add_argument("--num-entries", type=int, default=30)
    args = parser.parse_args()

    engine = Engine.from_str(args.engine)
    cw = CloudWatchClient(engine, args.cluster_id, None)
    df = cw.fetch_metrics(
        [(args.metric_name, args.metric_stat)],
        timedelta(seconds=args.period_s),
        args.num_entries,
    )
    print(df)
