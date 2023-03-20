import argparse
import sys

import brad
import brad.exec.admin
import brad.exec.cli
import brad.exec.server
import brad.exec.daemon


def main():
    parser = argparse.ArgumentParser(
        description="BRAD: A cloud-native federated database management system.",
    )
    parser.add_argument(
        "-v",
        "--version",
        action="store_true",
        help="Print BRAD's version and exit.",
    )
    subparsers = parser.add_subparsers(title="Commands")
    brad.exec.cli.register_command(subparsers)
    brad.exec.server.register_command(subparsers)
    brad.exec.daemon.register_command(subparsers)
    brad.exec.admin.register_command(subparsers)
    args = parser.parse_args()

    if args.version:
        print("BRAD", brad.__version__)

    if "func" not in args:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
