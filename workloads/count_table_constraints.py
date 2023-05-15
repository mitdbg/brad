import argparse
import yaml
import json
from pathlib import Path
from typing import Set
from brad.query_rep import QueryRep


def process_data(dir_path: Path, table_set: Set[str]):
    table_constraints = set()

    def process_raw_query(raw_query):
        query_rep = QueryRep(raw_query)
        relevant = list(filter(lambda t: t in table_set, query_rep.tables()))
        relevant.sort()
        relevant_tuple = tuple(relevant)
        table_constraints.add(relevant_tuple)

    for file_path in dir_path.iterdir():
        if file_path.suffix == ".json" and file_path.name != "log.json":
            with open(file_path) as json_file:
                data = json.load(json_file)
                for raw_query in data.values():
                    process_raw_query(raw_query)

        elif file_path.suffix == ".sql":
            with open(file_path) as sql_file:
                sql = sql_file.read()
                process_raw_query(sql.strip())

        else:
            continue  # Ignore other files

    return table_constraints


def main():
    parser = argparse.ArgumentParser(
        "Used to count the number of unique sets of table constraints in a query trace."
    )
    parser.add_argument("--oltp-dirs", type=str, nargs="+")
    parser.add_argument("--olap-dirs", type=str, nargs="+")
    parser.add_argument("--schema-file", type=str, required=True)
    args = parser.parse_args()

    with open(args.schema_file, "r") as schema_file:
        schema_yaml = yaml.load(schema_file, Loader=yaml.Loader)
        table_set = {raw["table_name"] for raw in schema_yaml["tables"]}

    constraint_sets = []

    for d in args.oltp_dirs:
        data_dir = Path(d)
        constraint_sets.append(process_data(data_dir, table_set))

    for d in args.olap_dirs:
        data_dir = Path(d)
        constraint_sets.append(process_data(data_dir, table_set))

    unique_constraints = set.union(*constraint_sets)
    print("Constraints:")
    for c in unique_constraints:
        print(c)
    print("Total constraints:", len(unique_constraints))


if __name__ == "__main__":
    main()
