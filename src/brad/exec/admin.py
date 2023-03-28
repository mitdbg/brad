import sys
import logging

from brad.utils import set_up_logging
from brad.admin.bootstrap_schema import bootstrap_schema
from brad.admin.drop_schema import drop_schema

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
        help="Path to BRAD's configuration file.",
    )
    parser.add_argument(
        "--bootstrap-schema-file",
        type=str,
        help="Path to the database schema to bootstrap.",
    )
    parser.add_argument(
        "--drop-schema-name",
        type=str,
        help="The name of the database schema to drop.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Set to enable debug logging.",
    )
    parser.set_defaults(func=main)


def main(args):
    set_up_logging(debug_mode=args.debug)

    if args.action == "bootstrap_schema":
        bootstrap_schema(args)
    elif args.action == "drop_schema":
        # NOTE: This will delete the data in the tables too!
        drop_schema(args)
    else:
        logger.error("Unknown admin action: %s", args.action)
        sys.exit(1)
