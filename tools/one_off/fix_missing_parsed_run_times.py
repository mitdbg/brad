import argparse
import shutil
import json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-file", type=str, required=True)
    args = parser.parse_args()

    with open(args.in_file, encoding="UTF-8") as file:
        orig = json.load(file)

    # Make fixes.
    for pp, pq in zip(orig["parsed_plans"], orig["parsed_queries"]):
        pq["plan_runtime"] = pp["plan_runtime"]

    shutil.copy2(args.in_file, args.in_file + "_orig")

    with open(args.in_file, "w", encoding="UTF-8") as file:
        json.dump(orig, file)


if __name__ == "__main__":
    main()
