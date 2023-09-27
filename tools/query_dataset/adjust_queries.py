import argparse
import asyncio
import yaml
import sqlglot
import sqlglot.expressions as exp
import numpy as np
import numpy.typing as npt
import random
from typing import Dict, List, Tuple

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.connection.connection import Connection
from brad.connection.factory import ConnectionFactory
from brad.provisioning.directory import Directory
from brad.query_rep import QueryRep

ColStats = Dict[str, Dict[str, Tuple[int, int]]]


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
            table_indexed_cols[name] = indexed
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
) -> ColStats:
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
    aurora_best_mask = (
        data[:, ei[Engine.Aurora]] * thres < data[:, ei[Engine.Redshift]]
    ) & (data[:, ei[Engine.Aurora]] * thres < data[:, ei[Engine.Athena]])
    athena_best_mask = (
        data[:, ei[Engine.Athena]] * thres < data[:, ei[Engine.Redshift]]
    ) & (data[:, ei[Engine.Athena]] * thres < data[:, ei[Engine.Aurora]])
    redshift_best_mask = (
        data[:, ei[Engine.Redshift]] * thres < data[:, ei[Engine.Aurora]]
    ) & (data[:, ei[Engine.Redshift]] * thres < data[:, ei[Engine.Athena]])
    remainder_mask = (~aurora_best_mask) & (~athena_best_mask) & (~redshift_best_mask)
    return athena_best_mask, aurora_best_mask, redshift_best_mask, remainder_mask, total


def redshift_to_aurora(
    query: str,
    ics: ColStats,
    prng: random.Random,
    tgt_selectivity: float,
    select_star_prob: float,
) -> str:
    """
    Converts a query that does well on Redshift to one that (should) do well on
    Aurora (using heuristics).
    """
    # - Remove all predicates on indexed columns.
    # - Collect tables involved in the join.
    # - Choose n - 1 tables to add selective predicates on; choose the predicate
    #   column randomly.
    # - Add back all predicates.
    # - Randomly change the query to SELECT * (want all columns).

    qr = QueryRep(query)
    ast = qr.ast()

    # Extract all predicates
    predicates = []
    for w in ast.find_all(exp.Where):
        for p in w.find_all(exp.Predicate):
            predicates.append(p)

    # Remove predicates on indexed columns.
    clean_predicates_str = []
    for p in predicates:
        if isinstance(p.left, exp.Column):
            # Check if it references an indexed column
            col_name = p.left.name
            table_name = p.left.table
            if col_name in ics[table_name]:
                continue

        if isinstance(p.right, exp.Column):
            # Check if it references an indexed column
            col_name = p.right.name
            table_name = p.right.table
            if col_name in ics[table_name]:
                continue
        clean_predicates_str.append(p.sql())

    # Generate selective predicates on n - 1 tables.
    tables = qr.tables()
    prng.shuffle(tables)
    if len(tables) > 1:
        tables.pop()

    sel_predicates_str = []
    for tbl in tables:
        indexed_cols = ics[tbl]
        col, ranges = prng.choice(list(indexed_cols.items()))
        # Uniformity assumption
        tr = ranges[1] - ranges[0] + 1
        upper = tr * (
            tgt_selectivity + prng.random() * 0.02
        )  # Add a bit of random jitter
        upper += ranges[0]
        upper = int(upper)
        sel_predicates_str.append(f'"{tbl}"."{col}" <= {upper}')

    modified = ast.where(*clean_predicates_str, *sel_predicates_str, append=False)

    if prng.random() < select_star_prob:
        modified = modified.select("*", append=False)

    return modified.sql()


def redshift_to_athena(query: str, ics: ColStats) -> str:
    """
    Converts a query that does well on Redshift to one that (should) do well on
    Athena (using heuristics).
    """
    # To implement.
    return query


def process_queries(
    query_file: str,
    recorded_run_times: str,
    engine_weights: List[int],
    threshold: float,
    ics: ColStats,
    prng: random.Random,
) -> None:
    with open(query_file) as file:
        queries = [line.strip() for line in file]
    recorded_rt = np.load(recorded_run_times)

    (
        athena_best_mask,
        aurora_best_mask,
        redshift_best_mask,
        remainder_mask,
        total,
    ) = compute_dataset_dist(recorded_rt, threshold)

    aurora_best = aurora_best_mask.sum()
    athena_best = athena_best_mask.sum()
    redshift_best = redshift_best_mask.sum()
    too_close = remainder_mask.sum()

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

    redshift_best_indices = np.where(redshift_best_mask)[0]
    assert len(redshift_best_indices) == redshift_best

    new_aurora = []
    for qidx in redshift_best_indices[:add_to_aurora]:
        orig_query = queries[qidx]
        new_query = redshift_to_aurora(
            orig_query, ics, prng, tgt_selectivity=0.05, select_star_prob=0.2
        )
        new_aurora.append((orig_query, new_query))

    with open("aurora_diff.sql", "w") as file:
        for orig, new in new_aurora:
            print(orig, file=file)
            print(new, file=file)
            print(file=file)

    new_athena = []
    for qidx in redshift_best_indices[add_to_aurora : add_to_athena + add_to_aurora]:
        orig_query = queries[qidx]
        new_query = redshift_to_athena(orig_query, ics)
        new_athena.append((orig_query, new_query))

    with open("athena_diff.sql", "w") as file:
        for orig, new in new_athena:
            print(orig, file=file)
            print(new, file=file)
            print(file=file)

    # Print out the new query file. We cluster the queries.
    with open("adjusted_queries.sql", "w") as file:
        # Athena
        # Original queries
        for qidx in np.where(athena_best_mask)[0]:
            print(queries[qidx], file=file)
        # Modified queries
        for _, modified in new_athena:
            print(modified, file=file)

        # Aurora
        # Original queries
        for qidx in np.where(aurora_best_mask)[0]:
            print(queries[qidx], file=file)
        # Modified queries
        for _, modified in new_aurora:
            print(modified, file=file)

        # Redshift remainder
        for qidx in redshift_best_indices[add_to_athena + add_to_aurora :]:
            print(queries[qidx], file=file)

        # Remainder queries
        for qidx in np.where(remainder_mask)[0]:
            print(queries[qidx], file=file)


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
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    table_indexed_cols = load_indexed_cols(args.schema_file)

    config = ConfigFile(args.config_file)
    directory = Directory(config)
    asyncio.run(directory.refresh())
    conn = ConnectionFactory.connect_to_sync(
        Engine.Aurora, args.schema_name, config, directory
    )
    indexed_col_stats = load_indexed_column_stats(conn, table_indexed_cols)
    conn.close_sync()

    print()
    print("Detected tables")
    print(list(indexed_col_stats.keys()))
    print()

    prng = random.Random(args.seed)
    process_queries(
        args.source_query_file,
        args.recorded_run_times,
        args.engine_weights,
        args.threshold,
        indexed_col_stats,
        prng,
    )


ei = {
    Engine.Athena: 0,
    Engine.Aurora: 1,
    Engine.Redshift: 2,
}


if __name__ == "__main__":
    main()
