import argparse
import json
import pathlib
import pandas as pd
import numpy as np

pd.options.mode.chained_assignment = None


def load_data(data_path, prefix):
    data = []

    for exp in pathlib.Path(data_path).iterdir():
        if exp.suffix != ".json":
            continue

        parts = exp.name.split("+")
        if parts[1] != prefix:
            continue

        with open(exp, "r", encoding="UTF-8") as file:
            raw_json = json.load(file)

        for query in raw_json:
            if query["status"] != "SUCCEEDED":
                continue

            qidx = query["query_index"]
            df = pd.DataFrame.from_records(
                query["physical"]["data"], columns=query["physical"]["cols"]
            )
            data.append((qidx, df))

    # Deduplicate. We had to restart the data collection a few times.
    dedup_dict = {}

    for qidx, df in data:
        dedup_dict[qidx] = (qidx, df)

    deduped = list(dedup_dict.values())
    deduped.sort(key=lambda tup: tup[0])
    return deduped


def extract_total_blocks_accessed(df):
    rel = df[df["relname"].str.endswith("_brad_source")]
    rel["blks_accessed"] = (
        rel["heap_blks_read"]
        + rel["heap_blks_hit"]
        + rel["idx_blks_read"]
        + rel["idx_blks_hit"]
    )
    return rel["blks_accessed"].sum()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-path", type=str, required=True)
    parser.add_argument("--prefix", type=str, required=True)
    parser.add_argument("--num-queries", type=int, default=5272)
    args = parser.parse_args()

    raw_data = load_data(args.data_path, args.prefix)
    data_accessed_per_query = map(
        lambda tup: (tup[0], extract_total_blocks_accessed(tup[1])), raw_data
    )

    # Write out as a numpy array.
    arr = np.full(args.num_queries, -1)
    for query_index, total_blocks_accessed in data_accessed_per_query:
        arr[query_index] = total_blocks_accessed

    np.save("all_queries_aurora_blocks_accessed.npy", arr)


if __name__ == "__main__":
    main()
