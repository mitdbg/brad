import argparse
import sys

import iohtap
import iohtap.exec.cli
import iohtap.exec.server


def main():
    parser = argparse.ArgumentParser(
        description="IOHTAP: A cloud-native federated database management system.",
    )
    parser.add_argument(
        "-v",
        "--version",
        action="store_true",
        help="Print IOHTAP's version and exit.",
    )
    subparsers = parser.add_subparsers(title="Commands")
    iohtap.exec.cli.register_command(subparsers)
    iohtap.exec.server.register_command(subparsers)
    args = parser.parse_args()

    if args.version:
        print("IOHTAP", iohtap.__version__)

    if "func" not in args:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
