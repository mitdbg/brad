import logging

logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "bootstrap_vdbes", help="Set up a new virtual infrastructure on BRAD."
    )
    parser.add_argument(
        "--physical-config-file",
        type=str,
        required=True,
        help="Path to BRAD's physical configuration file.",
    )
    parser.add_argument(
        "--vdbe-file",
        type=str,
        required=True,
        help="Path to the virtual infrastructure definition to boostrap.",
    )
    parser.set_defaults(admin_action=bootstrap_vdbes)


# This method is called by `brad.exec.admin.main`.
def bootstrap_vdbes(_args) -> None:
    logger.info("Running VDBE boostrap.")
