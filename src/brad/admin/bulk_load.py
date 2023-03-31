import logging

logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser("bulk_load", help="Bulk load table(s) on BRAD.")
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
        help="The name of the schema that contains the table(s) to load.",
    )
    parser.add_argument(
        "--manifest-file",
        type=str,
        required=True,
        help="The path to a manifest file, which contains the tables to load.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bulk load is normally only allowed when the table(s) are all empty. "
        "Set this flag to override this restriction.",
    )
    parser.set_defaults(admin_action=bulk_load)


def bulk_load(args) -> None:
    pass
