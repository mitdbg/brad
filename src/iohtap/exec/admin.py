import sys
import logging

from iohtap.utils import set_up_logging
from iohtap.admin.set_up_tables import set_up_tables
from iohtap.admin.tear_down_tables import tear_down_tables

logger = logging.getLogger(__name__)


def register_command(subparsers):
    parser = subparsers.add_parser(
        "admin",
        help="Used to run administrative tasks on the underlying engines.",
    )
    parser.add_argument("action", type=str, help="The administrative task to run.")
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to IOHTAP's configuration file.",
    )
    parser.add_argument(
        "--schema-file",
        type=str,
        required=True,
        help="Path to the database schema.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Set to enable debug logging.",
    )
    parser.set_defaults(func=main)


def main(args):
    set_up_logging(debug_mode=args.debug)

    if args.action == "set_up_tables":
        set_up_tables(args)
    elif args.action == "tear_down_tables":
        # NOTE: This will delete the data in the tables too!
        tear_down_tables(args)
    else:
        logger.error("Unknown admin action: %s", args.action)
        sys.exit(1)
