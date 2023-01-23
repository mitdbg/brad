import argparse

import iohtap


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
    args = parser.parse_args()

    if args.version:
        print("IOHTAP", iohtap.__version__)


if __name__ == "__main__":
    main()
