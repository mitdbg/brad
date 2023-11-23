import argparse
import json


def main():
    # Used to split the parsed query run file.
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-parsed", type=str, required=True)
    parser.add_argument("--queries-file", type=str, required=True)
    parser.add_argument("--out-file-1", type=str, required=True)
    parser.add_argument("--out-file-2", type=str)
    args = parser.parse_args()

    with open(args.source_parsed, "r", encoding="UTF-8") as file:
        source = json.load(file)

    with open(args.queries_file, "r", encoding="UTF-8") as file:
        queries = {line.strip() for line in file}

    matched_pp, matched_pq, matched_sql, matched_da = [], [], [], []
    unmatched_pp, unmatched_pq, unmatched_sql, unmatched_da = [], [], [], []

    for pp, pq, sql, da in zip(
        source["parsed_plans"],
        source["parsed_queries"],
        source["sql_queries"],
        source["bytes_scanned"],
    ):
        if not sql.endswith(";"):
            match_sql = sql + ";"
        else:
            match_sql = sql

        if match_sql in queries:
            matched_pp.append(pp)
            matched_pq.append(pq)
            matched_sql.append(sql)
            matched_da.append(da)
        else:
            unmatched_pp.append(pp)
            unmatched_pq.append(pq)
            unmatched_sql.append(sql)
            unmatched_da.append(da)

    print("Matching:", len(matched_pp))
    print("Unmatched:", len(unmatched_pp))

    matched_result = {
        **source,
        "parsed_plans": matched_pp,
        "parsed_queries": matched_pq,
        "sql_queries": matched_sql,
        "bytes_scanned": matched_da,
    }

    with open(args.out_file_1, "w", encoding="UTF-8") as file:
        json.dump(matched_result, file)

    if args.out_file_2 is None:
        print(
            "No unmatched output file provided. Not serializing the unmatched data points."
        )
        return

    unmatched_result = {
        **source,
        "parsed_plans": unmatched_pp,
        "parsed_queries": unmatched_pq,
        "sql_queries": unmatched_sql,
        "bytes_scanned": unmatched_da,
    }

    with open(args.out_file_2, "w", encoding="UTF-8") as file:
        json.dump(unmatched_result, file)


if __name__ == "__main__":
    main()
