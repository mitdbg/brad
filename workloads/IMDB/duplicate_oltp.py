import argparse
import random
import pathlib
import json


def shuffle_workload(txns, prng):
    values = list(txns.values())
    prng.shuffle(values)
    return {key: value for key, value in zip(txns.keys(), values)}


def main():
    parser = argparse.ArgumentParser("Duplicates the transactional workload.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--path", type=str, default="./OLTP_queries")
    parser.add_argument("--stop-idx", type=int, default=16)
    args = parser.parse_args()

    prng = random.Random(args.seed)

    workload_dir = pathlib.Path(args.path)
    file_template = "transaction_user_{}.json"

    for idx in range(1, args.stop_idx + 1):
        print("Processing", idx)
        with open(workload_dir / file_template.format(idx), "r") as file:
            txns = json.load(file)

        # We shuffle the values but preserve the keys.
        shuffled = shuffle_workload(txns, prng)

        with open(
            workload_dir / file_template.format(idx + args.stop_idx), "w"
        ) as file:
            json.dump(shuffled, file)


if __name__ == "__main__":
    main()
