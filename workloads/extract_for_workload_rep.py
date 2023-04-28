import argparse
import json
import random
from pathlib import Path


def process_data(dir_path: Path, is_transactional: bool, sampling_probability: float):
    queries = []

    for file_path in dir_path.iterdir():
        if file_path.suffix == ".json" and file_path.name != "log.json":
            with open(file_path) as json_file:
                data = json.load(json_file)
                if is_transactional:
                    for value in data.values():
                        if random.random() <= sampling_probability:
                            if value == "COMMIT;":
                                continue
                            queries.append(value)
                else:
                    for value in data.values():
                        queries.append(value)

        elif file_path.suffix == ".sql":
            with open(file_path) as sql_file:
                sql = sql_file.read()
                queries.append(sql.strip())

        else:
            continue  # Ignore other files

    return queries


def main():
    parser = argparse.ArgumentParser(
        "Used to extract the queries from a trace for import into the blueprint planner."
    )
    parser.add_argument("--oltp-dirs", type=str, nargs="+")
    parser.add_argument("--olap-dirs", type=str, nargs="+")
    parser.add_argument("--sample-freq", type=float, default=0.01)
    parser.add_argument("--output-dir", type=str)
    args = parser.parse_args()

    txn_queries = []
    olap_queries = []

    for file_path in args.oltp_dirs:
        txn_queries.extend(
            process_data(
                Path(file_path),
                is_transactional=True,
                sampling_probability=args.sample_freq,
            )
        )
    for file_path in args.olap_dirs:
        olap_queries.extend(
            process_data(
                Path(file_path),
                is_transactional=False,
                sampling_probability=args.sample_freq,
            )
        )

    out_dir = Path(args.output_dir)
    with open(out_dir / "oltp.sql", "w") as out:
        for q in txn_queries:
            print(q, file=out)
    with open(out_dir / "olap.sql", "w") as out:
        for q in olap_queries:
            print(q, file=out)


if __name__ == "__main__":
    main()
