import argparse
import asyncio
import yaml
import sqlglot
import sqlglot.expressions as exp
import numpy as np
import numpy.typing as npt
from typing import Dict, List, Tuple

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.connection.connection import Connection
from brad.connection.factory import ConnectionFactory
from brad.provisioning.directory import Directory


def load_indexed_cols(schema_file: str) -> Dict[str, List[str]]:
    with open(schema_file) as file:
        raw_schema = yaml.load(file, yaml.Loader)

    # Retrieve all tables indexed columns in the schema
    # Note that we only index numeric columns, so this is OK for this script.
    table_indexed_cols = {}
    for table in raw_schema["tables"]:
        name = table["table_name"]
        indexed = []

        for column in table["columns"]:
            if "primary_key" in column and column["primary_key"]:
                indexed.append(column["name"])

        if "indexes" not in table:
            continue

        for index in table["indexes"]:
            parts = index.split(",")
            if len(parts) > 1:
                # Skip composite indexes.
                continue
            indexed.append(parts[0])

        table_indexed_cols[name] = indexed
    return table_indexed_cols


def load_indexed_column_stats(
    connection: Connection, table_indexed_cols: Dict[str, List[str]]
) -> Dict[str, Dict[str, Tuple[int, int]]]:
    cursor = connection.cursor_sync()
    table_info = {}

    for table_name, indexed_cols in table_indexed_cols.items():
        column_info = {}
        for column in indexed_cols:
            cursor.execute_sync(
                f"SELECT MIN({column}), MAX({column}) FROM {table_name};"
            )
            min_value, max_value = cursor.fetchone_sync()
            if min_value is None or max_value is None:
                print("NOTE: Column has all NULLs: {}.{}".format(table_name, column))
                continue
            column_info[column] = (min_value, max_value)

        table_info[table_name] = column_info

    return table_info


def get_best_indices(data: npt.NDArray, engine: Engine, thres: float) -> npt.NDArray:
    all_engines = [Engine.Redshift, Engine.Aurora, Engine.Athena]
    all_engines.remove(engine)

    mask1 = data[:, ei[engine]] * thres < data[:, ei[all_engines[0]]]
    mask2 = data[:, ei[engine]] * thres < data[:, ei[all_engines[1]]]
    comb = mask1 & mask2
    return np.where(comb)[0]


def compute_dataset_dist(
    data: npt.NDArray, thres: float
) -> Tuple[int, int, int, int, int]:
    total, _ = data.shape
    aurora_best = (
        (data[:, ei[Engine.Aurora]] * thres < data[:, ei[Engine.Redshift]])
        & (data[:, ei[Engine.Aurora]] * thres < data[:, ei[Engine.Athena]])
    ).sum()
    athena_best = (
        (data[:, ei[Engine.Athena]] * thres < data[:, ei[Engine.Redshift]])
        & (data[:, ei[Engine.Athena]] * thres < data[:, ei[Engine.Aurora]])
    ).sum()
    redshift_best = (
        (data[:, ei[Engine.Redshift]] * thres < data[:, ei[Engine.Aurora]])
        & (data[:, ei[Engine.Redshift]] * thres < data[:, ei[Engine.Athena]])
    ).sum()
    too_close = total - aurora_best - athena_best - redshift_best
    return athena_best, aurora_best, redshift_best, too_close, total


def redshift_to_aurora(query: str) -> str:
    """
    Converts a query that does well on Redshift to one that (should) do well on
    Aurora (using heuristics).
    """
    pass


def redshift_to_athena(query: str) -> str:
    """
    Converts a query that does well on Redshift to one that (should) do well on
    Athena (using heuristics).
    """
    pass


def process_queries(
    query_file: str,
    recorded_run_times: str,
    engine_weights: List[int],
    threshold: float,
) -> None:
    with open(query_file) as file:
        queries = [line.strip() for line in file]
    recorded_rt = np.load(recorded_run_times)

    athena_best, aurora_best, redshift_best, too_close, total = compute_dataset_dist(
        recorded_rt, threshold
    )

    print("Recorded distributon:")
    print("Threshold:", threshold)
    print("Athena best:", athena_best)
    print("Aurora best:", aurora_best)
    print("Redshift best:", redshift_best)
    print("Other:", too_close)
    print("Total:", total)

    # This code is written with the assumption that Redshift is overbalanced.
    assert redshift_best > aurora_best, "Assumption"
    assert redshift_best > athena_best, "Assumption"

    relevant = athena_best + aurora_best + redshift_best
    total_weight = sum(engine_weights)

    athena_weight = engine_weights[ei[Engine.Athena]] / total_weight
    aurora_weight = engine_weights[ei[Engine.Aurora]] / total_weight

    expected_athena = int(athena_weight * relevant)
    expected_aurora = int(aurora_weight * relevant)
    expected_redshift = relevant - expected_athena - expected_aurora

    print()
    print("Expected distribution:")
    print("Threshold:", threshold)
    print("Athena best:", expected_athena)
    print("Aurora best:", expected_aurora)
    print("Redshift best:", expected_redshift)
    print("Total relevant:", relevant)

    add_to_athena = expected_athena - athena_best
    add_to_aurora = expected_aurora - aurora_best

    print("Add to Aurora:", add_to_aurora)
    print("Add to Athena:", add_to_athena)

    assert add_to_aurora + add_to_athena < redshift_best

    redshift_best_indices = get_best_indices(recorded_rt, Engine.Redshift, threshold)
    assert len(redshift_best_indices) == redshift_best

    new_aurora = []
    for qidx in redshift_best_indices[:add_to_aurora]:
        orig_query = queries[qidx]
        new_query = redshift_to_aurora(orig_query)
        new_aurora.append((orig_query, new_query))

    new_athena = []
    for qidx in redshift_best_indices[add_to_aurora:add_to_athena + add_to_aurora]:
        orig_query = queries[qidx]
        new_query = redshift_to_athena(orig_query)
        new_athena.append((orig_query, new_query))

    # TODO: Write out the new queries.


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", type=str, required=True)
    parser.add_argument("--schema-name", type=str, default="imdb_extended_20g")
    parser.add_argument("--schema-file", type=str, required=True)
    parser.add_argument("--source-query-file", type=str, required=True)
    parser.add_argument("--recorded-run-times", type=str, required=True)
    parser.add_argument(
        "--engine-weights",
        type=int,
        nargs=len(ei),
        help="Best engine distribution weights: {Athena, Aurora, Redshift}.",
        default=[1, 1, 1],
    )
    parser.add_argument("--threshold", type=float, default=1.5)
    args = parser.parse_args()

    """
    table_indexed_cols = load_indexed_cols(args.schema_file)

    config = ConfigFile(args.config_file)
    directory = Directory(config)
    asyncio.run(directory.refresh())
    conn = ConnectionFactory.connect_to_sync(
        Engine.Aurora, args.schema_name, config, directory
    )
    indexed_col_stats = load_indexed_column_stats(conn, table_indexed_cols)
    conn.close_sync()

    for table, info in indexed_col_stats.items():
        print(f"Table: {table}")
        for column, data in info.items():
            print(f"  Column: {column}, Min: {data[0]}, Max: {data[1]}")
        print()
    """

    process_queries(
        args.source_query_file,
        args.recorded_run_times,
        args.engine_weights,
        args.threshold,
    )


ei = {
    Engine.Athena: 0,
    Engine.Aurora: 1,
    Engine.Redshift: 2,
}


if __name__ == "__main__":
    main()
