import enum
import pytz
from datetime import datetime

from brad.config.file import ConfigFile
from brad.planner.workload.builder import WorkloadBuilder


class Actions(enum.Enum):
    Inspect = "inspect"

    @staticmethod
    def from_str(candidate: str) -> "Actions":
        if candidate == Actions.Inspect.value:
            return Actions.Inspect
        raise ValueError("Unsupported action '{}'".format(str(candidate)))


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "workload_logs",
        help="Used to interact with workload logs (typically for debugging purposes).",
    )
    parser.add_argument(
        "action",
        choices=[Actions.Inspect.value],
        help="The workload log action to take.",
    )
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to BRAD's configuration file.",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        required=True,
        help="The name of the schema.",
    )
    parser.add_argument(
        "--window-start",
        type=str,
        help="Workload window start timestamp (UTC) in YYYY-MM-DD HH:MM:SS format.",
    )
    parser.add_argument(
        "--window-end",
        type=str,
        help="Workload window end timestamp (UTC) in YYYY-MM-DD HH:MM:SS format.",
    )
    parser.set_defaults(admin_action=workload_logs)


# This method is called by `brad.exec.admin.main`.
def workload_logs(args) -> None:
    action = Actions.from_str(args.action)
    if action == Actions.Inspect:
        inspect_logs(args)


def inspect_logs(args) -> None:
    config = ConfigFile.load(args.config_file)

    timestamp_format = "%Y-%m-%d %H:%M:%S"
    window_start = datetime.strptime(args.window_start, timestamp_format)
    window_end = datetime.strptime(args.window_end, timestamp_format)

    window_start = window_start.replace(tzinfo=pytz.utc)
    window_end = window_end.replace(tzinfo=pytz.utc)

    workload = (
        WorkloadBuilder()
        .add_queries_from_s3_logs(config, window_start, window_end)
        .build()
    )

    print("Window start:", window_start)
    print("Window end:", window_end)
    print()

    print("Period:", workload.period())
    print("Unique A queries:", len(workload.analytical_queries()))
    print("Unique (sampled) T queries:", len(workload.transactional_queries()))
    print()

    print("First 10 A queries:")
    for idx, q in enumerate(workload.analytical_queries()):
        if idx >= 10:
            break
        print("---")
        print("Query:", q.raw_query)
        print("Arrival count:", q.arrival_count())

    print()
    print("First 10 T queries:")
    for idx, q in enumerate(workload.transactional_queries()):
        if idx >= 10:
            break
        print("Query:", q.raw_query)
