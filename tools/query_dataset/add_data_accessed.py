import argparse
import json
import numpy as np

from brad.config.engine import Engine


def main() -> None:
    # This script replaces the "plan_runtime" key in the parsed query dataset
    # with the amount of data accessed. We use this for later training.
    parser = argparse.ArgumentParser()
    parser.add_argument("--all-queries-file", type=str, required=True)
    parser.add_argument("--data-accessed-file", type=str, required=True)
    parser.add_argument("--parsed-queries-file", type=str, required=True)
    parser.add_argument("--out-file", type=str, required=True)
    parser.add_argument("--engine", type=str, required=True)
    parser.add_argument("--take-log", action="store_true")
    parser.add_argument("--convert-mega", action="store_true")
    args = parser.parse_args()

    if args.take_log and args.convert_mega:
        print("WARNING: Both --take-log and --convert-mega set.")

    print("Processing:", args.parsed_queries_file)
    print("Using:", args.data_accessed_file)

    engine = Engine.from_str(args.engine)
    ei = {
        Engine.Athena: 0,
        Engine.Aurora: 1,
    }

    if engine == Engine.Redshift:
        raise RuntimeError("Unsupported.")

    with open(args.all_queries_file, encoding="UTF-8") as file:
        queries = {line.strip(): idx for idx, line in enumerate(file)}

    data_stats = np.load(args.data_accessed_file)
    data_stats_float = data_stats.astype(np.float64)

    if args.take_log:
        mask = (
            (data_stats_float > 0)
            & (~np.isnan(data_stats_float))
            & (~np.isinf(data_stats_float))
        )
        np.putmask(data_stats_float, mask, np.log(data_stats_float))

    with open(args.parsed_queries_file, encoding="UTF-8") as file:
        parsed = json.load(file)

    print("Sizes:")
    print("Queries:", len(queries))
    print("Parsed Plans:", len(parsed["parsed_plans"]))
    print("Parsed Queries:", len(parsed["parsed_queries"]))

    assert len(queries) == data_stats_float.shape[0], "Mismatched dataset."
    assert len(parsed["parsed_plans"]) == len(
        parsed["parsed_queries"]
    ), "Mismatched dataset."
    # assert len(parsed["parsed_queries"]) <= len(queries), "Invalid dataset."
    # assert len(parsed["parsed_plans"]) <= len(queries), "Invalid dataset."

    new_pp, new_pq, new_sql = [], [], []

    for pp, pq, sql in zip(
        parsed["parsed_plans"], parsed["parsed_queries"], parsed["sql_queries"]
    ):
        try:
            if not sql.endswith(";"):
                key_sql = sql + ";"
            else:
                key_sql = sql
            orig_idx = queries[key_sql]
            data_stat = data_stats_float[orig_idx, ei[engine]].item()

            if data_stat <= 0 or np.isnan(data_stat) or np.isinf(data_stat):
                continue

            if args.convert_mega:
                # We use MB instead of MiB (to be consistent with storage
                # usage). Billing is also done in MB.
                data_stat /= 1e6

            pp["plan_runtime"] = data_stat
            pq["plan_runtime"] = data_stat
            new_pp.append(pp)
            new_pq.append(pq)
            new_sql.append(sql)

        except KeyError:
            pass

    modified = {
        **parsed,
        "parsed_plans": new_pp,
        "parsed_queries": new_pq,
        "sql_queries": new_sql,
    }

    with open(args.out_file, "w", encoding="UTF-8") as file:
        json.dump(modified, file)


if __name__ == "__main__":
    main()
