import argparse
import pathlib
import random

TABLES = [
    "customer",
    "lineitem",
    "nation",
    "part",
    "partsupp",
    "region",
    "supplier",
    "orders",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=str, required=True)
    parser.add_argument("--min-epoch-value", type=int, default=0)
    parser.add_argument("--max-epoch-value", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=1337)
    args = parser.parse_args()

    random.seed(args.seed)
    data_dir = pathlib.Path(args.data_dir)

    for tbl in TABLES:
        print("Processing", tbl)
        with open(data_dir / "{}.tbl".format(tbl), "r") as inp, open(
            data_dir / "{}-epochs.tbl".format(tbl), "w"
        ) as out:
            for row in inp:
                lower = random.randrange(args.min_epoch_value, args.max_epoch_value)
                upper = random.randrange(lower, args.max_epoch_value)
                print("".join([row.strip(), str(lower), "|", str(upper)]), file=out)


if __name__ == "__main__":
    main()
