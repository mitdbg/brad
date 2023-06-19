import argparse
import pathlib
import json
import numpy as np
import pandas as pd
from typing import Any, Dict


def extract_relevant(raw_json: Dict[Any, Any]) -> pd.DataFrame:
    rows = []

    for item in raw_json:
        query_idx = item["query_index"]
        if item["status"] != "SUCCEEDED":
            rows.append((query_idx, -1, -1, -1, -1))
            continue

        stats = item["exec_info"]["Statistics"]
        row_stats = item["runtime_stats"]
        if "Rows" not in row_stats:
            input_rows = -1
        else:
            input_rows = row_stats["Rows"]["InputBytes"]

        rows.append(
            (
                query_idx,
                stats["TotalExecutionTimeInMillis"],
                stats["DataScannedInBytes"],
                input_rows,
            )
        )

    return pd.DataFrame.from_records(
        rows, columns=["query_index", "run_time_ms", "scanned_bytes", "input_rows"]
    )


def load_all_data(data_path: str, prefix: str) -> pd.DataFrame:
    dfs = []

    for file in pathlib.Path(data_path).iterdir():
        if not file.name.startswith("athena+{}".format(prefix)):
            continue
        with open(file, "r") as f:
            raw = json.load(f)
        dfs.append(extract_relevant(raw))

    return pd.concat(dfs, ignore_index=True).sort_values(by=["query_index"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-path", type=str, required=True)
    parser.add_argument("--prefix", type=str, required=True)
    args = parser.parse_args()

    df = load_all_data(args.data_path, args.prefix)
    assert len(df[df["scanned_bytes"] < 0]) == 0

    as_np = np.array(df["scanned_bytes"])
    np.save("all_queries_athena_scanned_bytes.npy", as_np)


if __name__ == "__main__":
    main()
