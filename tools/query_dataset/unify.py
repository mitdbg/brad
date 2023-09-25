import argparse
import json
import pathlib
import numpy as np
import numpy.typing as npt
import pandas as pd
import shutil

from typing import Any, List, Optional, Dict, Tuple
from brad.config.engine import Engine

pd.options.mode.chained_assignment = None

ParsedData = Dict[str, Any]


def load_queries(file_path: str) -> Tuple[List[str], Dict[str, int]]:
    with open(file_path) as file:
        queries = [line.strip() for line in file]
    return queries, {q: idx for idx, q in enumerate(queries)}


def load_raw_json(file_path: Optional[str]) -> Optional[ParsedData]:
    if file_path is None:
        return None
    with open(file_path) as file:
        return json.load(file)


def extract_run_times(
    qorder: Dict[str, int], parsed: Optional[ParsedData]
) -> npt.NDArray:
    run_times = np.ones(len(qorder), float)
    run_times *= np.nan

    if parsed is None:
        return run_times

    for idx, sql in enumerate(parsed["sql_queries"]):
        try:
            if not sql.endswith(";"):
                matching_sql = sql + ";"
            else:
                matching_sql = sql

            orig_idx = qorder[matching_sql]
            run_times[orig_idx] = parsed["parsed_plans"][idx]["plan_runtime"] / 1000
        except KeyError:
            print("WARNING: Unable to match", sql)

    return run_times


def has_data_scanned_stats(parsed: Optional[ParsedData]) -> bool:
    if parsed is None:
        return False
    return "blocks_accessed" in parsed or "bytes_scanned" in parsed


def extract_data_stats(
    qorder: Dict[str, int], parsed: Optional[ParsedData], engine: Engine
) -> npt.NDArray:
    if engine != Engine.Aurora and engine != Engine.Athena:
        raise RuntimeError(f"No data access stats for {repr(engine)}")

    data = np.ones(len(qorder), np.int64)
    data *= -1

    if parsed is None:
        return data

    data_key = "blocks_accessed" if engine == Engine.Aurora else "bytes_scanned"

    for idx, sql in enumerate(parsed["sql_queries"]):
        try:
            if not sql.endswith(";"):
                matching_sql = sql + ";"
            else:
                matching_sql = sql

            orig_idx = qorder[matching_sql]
            data[orig_idx] = parsed[data_key][idx]
        except KeyError:
            print("WARNING: Unable to match", sql)

    return data


def extract_relevant_athena(raw_json: Dict[Any, Any]) -> pd.DataFrame:
    rows = []

    for item in raw_json:
        query_idx = item["query_index"]
        if item["status"] != "SUCCEEDED":
            rows.append((query_idx, -1, -1, -1))
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

    # Deduplicate. We had to restart the data collection a few times.
    dedup_dict = {}

    for row in rows:
        qidx = row[0]
        if qidx not in dedup_dict:
            dedup_dict[qidx] = row
        else:
            # Replace the row we have stored if the current data point did not
            # time out.
            curr = dedup_dict[qidx]
            if curr[2] == -1 and row[2] != -1:
                dedup_dict[qidx] = row

    deduped = list(dedup_dict.values())
    deduped.sort(key=lambda tup: tup[0])

    return pd.DataFrame.from_records(
        rows, columns=["query_index", "run_time_ms", "scanned_bytes", "input_rows"]
    )


def load_athena_data(file_paths: List[str]) -> Optional[pd.DataFrame]:
    if len(file_paths) == 0:
        return None

    dfs = []

    for file in file_paths:
        with open(file, "r") as f:
            raw = json.load(f)
        dfs.append(extract_relevant_athena(raw))

    all_data = pd.concat(dfs, ignore_index=True).sort_values(by=["query_index"])
    deduped = all_data.groupby("query_index").max().reset_index()
    return deduped


def load_aurora_data(file_paths: List[str]) -> Optional[pd.DataFrame]:
    if len(file_paths) == 0:
        return None

    data = []

    for exp in file_paths:
        with open(exp, "r") as file:
            raw_json = json.load(file)

        for query in raw_json:
            if query["status"] != "SUCCEEDED":
                continue

            qidx = query["query_index"]
            df = pd.DataFrame.from_records(
                query["physical"]["data"], columns=query["physical"]["cols"]
            )
            data.append((qidx, df))

    # Deduplicate if needed.
    dedup_dict = {}

    for qidx, df in data:
        dedup_dict[qidx] = (qidx, df)

    deduped = list(dedup_dict.values())
    deduped.sort(key=lambda tup: tup[0])
    return deduped


def extract_total_blocks_accessed(df: pd.DataFrame) -> int:
    rel = df[df["relname"].str.endswith("_brad_source")]
    rel["blks_accessed"] = (
        rel["heap_blks_read"]
        + rel["heap_blks_hit"]
        + rel["idx_blks_read"]
        + rel["idx_blks_hit"]
    )
    return rel["blks_accessed"].sum()


def consolidate_aurora_data_accessed(
    qorder: Dict[str, int], recorded_data: Optional[pd.DataFrame]
) -> npt.NDArray:
    arr = np.full(len(qorder), -1)

    if recorded_data is None:
        return arr

    data_accessed_per_query = map(
        lambda tup: (tup[0], extract_total_blocks_accessed(tup[1])), recorded_data
    )

    # Write out as a numpy array.
    for query_index, total_blocks_accessed in data_accessed_per_query:
        arr[query_index] = total_blocks_accessed

    return arr


def consolidate_athena_data_accessed(
    qorder: Dict[str, int], recorded_data: Optional[pd.DataFrame]
) -> npt.NDArray:
    arr = np.full(len(qorder), -1)

    if recorded_data is None:
        return arr

    arr[recorded_data["query_index"]] = recorded_data["scanned_bytes"]
    return arr


def main():
    """
    This script is used to unify the collected query data.
    Inputs:
      - File containing list of queries
      - Parsed queries (Aurora, Redshift, Athena)
      - Data accessed statistics (Aurora, Athena)
    Outputs:
      - Ordered parsed queries file
      - Query run times as a Numpy array
      - Data access statistics as a Numpy array
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--queries-file", type=str, required=True)
    parser.add_argument("--out-dir", type=str, default=".")
    parser.add_argument("--parsed-athena", type=str)
    parser.add_argument("--parsed-aurora", type=str)
    parser.add_argument("--parsed-redshift", type=str)
    parser.add_argument("--raw-athena-scan-data", type=str, nargs="*")
    parser.add_argument("--raw-aurora-scan-data", type=str, nargs="*")
    args = parser.parse_args()

    out_dir = pathlib.Path(args.out_dir)

    _, qorder = load_queries(args.queries_file)
    athena_parsed = load_raw_json(args.parsed_athena)
    aurora_parsed = load_raw_json(args.parsed_aurora)
    redshift_parsed = load_raw_json(args.parsed_redshift)

    athena_rt = extract_run_times(qorder, athena_parsed)
    aurora_rt = extract_run_times(qorder, aurora_parsed)
    redshift_rt = extract_run_times(qorder, redshift_parsed)

    run_times = np.stack([athena_rt, aurora_rt, redshift_rt])
    run_times = np.transpose(run_times)

    if has_data_scanned_stats(athena_parsed):
        athena_data = extract_data_stats(qorder, athena_parsed, Engine.Athena)
    else:
        # Athena: Bytes accessed
        athena_raw = load_athena_data(args.raw_athena_scan_data)
        athena_data = consolidate_athena_data_accessed(qorder, athena_raw)

    if has_data_scanned_stats(aurora_parsed):
        aurora_data = extract_data_stats(qorder, aurora_parsed, Engine.Aurora)
    else:
        # Aurora: Blocks accessed
        aurora_raw = load_aurora_data(args.raw_aurora_scan_data)
        aurora_data = consolidate_aurora_data_accessed(qorder, aurora_raw)

    data_accessed = np.stack([athena_data, aurora_data])
    data_accessed = np.transpose(data_accessed)

    shutil.copy2(args.queries_file, out_dir / "queries.sql")
    np.save(out_dir / "run_time_s-athena-aurora-redshift.npy", run_times)
    np.save(out_dir / "data_accessed-athena-aurora.npy", data_accessed)


if __name__ == "__main__":
    main()
