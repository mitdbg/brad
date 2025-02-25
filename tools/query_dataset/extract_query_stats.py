import asyncio
import argparse
import json
import sys
import shutil
import pathlib
from tqdm import tqdm
from typing import Any, Dict, List

from brad.config.file import ConfigFile
from brad.connection.factory import ConnectionFactory
from brad.data_stats.plan_parsing import (
    parse_explain_verbose,
    extract_base_cardinalities,
)


def load_all_queries(queries_file: str) -> List[str]:
    with open(queries_file, "r", encoding="UTF-8") as file:
        return [line.strip() for line in file]


def base_table_name(table: str) -> str:
    suffix = "_brad_source"
    suffix_len = len(suffix)
    return table[:-suffix_len]


def get_table_names(cursor) -> List[str]:
    # Retrieve the table names
    cursor.execute_sync(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    )

    # Fetch all the table names
    table_names = cursor.fetchall_sync()

    return [tn[0] for tn in table_names]


def get_table_sizes(cursor, table_names) -> Dict[str, int]:
    table_counts = {}

    # Iterate over the table names and retrieve the row count for each table
    for table_name in table_names:
        # Execute a count query for each table
        cursor.execute_sync(f"SELECT COUNT(*) FROM {table_name}")

        # Fetch the row count
        row_count = cursor.fetchone_sync()[0]

        # Store the table name and row count in the dictionary
        if table_name.endswith("_brad_source"):
            btn = base_table_name(table_name)
        else:
            btn = table_name
        table_counts[btn] = row_count

    return table_counts


def get_table_widths(cursor, table_names) -> Dict[str, int]:
    table_widths = {}

    for table_name in table_names:
        if table_name.endswith("_brad_source"):
            tn = base_table_name(table_name)
        else:
            tn = table_name

        cursor.execute_sync(f"EXPLAIN VERBOSE SELECT * FROM {tn}")
        plan_lines = [row[0] for row in cursor.fetchall_sync()]

        plan = parse_explain_verbose(plan_lines)
        bc = extract_base_cardinalities(plan)
        assert len(bc) == 1

        table_widths[tn] = bc[0].width

    return table_widths


def save_checkpoint(data: List[Dict[str, Any]], output_file: pathlib.Path):
    temp_path = output_file.with_name(output_file.name + "_temp")
    with open(temp_path, "w", encoding="UTF-8") as outfile:
        json.dump(data, outfile, indent=2, default=str)
    shutil.move(temp_path, output_file)


def main():
    # Used to extract query features for the forest-based router.
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", type=str, required=True)
    parser.add_argument("--schema-name", type=str, required=True)
    parser.add_argument("--queries-file", type=str, required=True)
    args = parser.parse_args()

    config = ConfigFile.load(args.config_file)
    conn = asyncio.run(ConnectionFactory.connect_to_sidecar(args.schema_name, config))
    cursor = conn.cursor_sync()

    # Make sure stats are up to date.
    print("Running vacuum + analyze...", file=sys.stderr, flush=True)
    cursor.execute_sync("VACUUM ANALYZE")

    print("Loading queries...", file=sys.stderr, flush=True)
    queries = load_all_queries(args.queries_file)

    print("Getting all table names...", file=sys.stderr, flush=True)
    table_names = get_table_names(cursor)

    print("Getting table sizes...", file=sys.stderr, flush=True)
    table_sizes = get_table_sizes(cursor, table_names)
    print(json.dumps(table_sizes, indent=2), file=sys.stderr, flush=True)

    print("Getting table widths...", file=sys.stderr, flush=True)
    table_widths = get_table_widths(cursor, table_names)
    print(json.dumps(table_widths, indent=2), file=sys.stderr, flush=True)

    print("Running...", file=sys.stderr, flush=True)
    results = []
    for qidx, q in enumerate(tqdm(queries)):
        cursor.execute_sync("EXPLAIN VERBOSE " + q)
        plan_lines = [row[0] for row in cursor.fetchall_sync()]

        plan = parse_explain_verbose(plan_lines)
        base_cards = extract_base_cardinalities(plan)

        table_selectivity = {}
        table_cardinality = {}
        table_access_widths = {}
        table_access_widths_pct = {}
        table_access_method = {}

        for bc in base_cards:
            table_name = base_table_name(bc.table_name)
            total_tups = table_sizes[table_name]

            selectivity = bc.cardinality / total_tups
            selectivity = min(selectivity, 1.0)

            total_width = table_widths[table_name]
            access_width_pct = bc.width / total_width
            access_width = bc.width

            if table_name in table_selectivity:
                table_selectivity[table_name] = max(
                    table_selectivity[table_name], selectivity
                )
            else:
                table_selectivity[table_name] = selectivity

            if table_name in table_access_widths:
                table_access_widths[table_name] = max(
                    table_access_widths[table_name], access_width
                )
            else:
                table_access_widths[table_name] = access_width

            if table_name in table_access_widths_pct:
                table_access_widths_pct[table_name] = max(
                    table_access_widths_pct[table_name], access_width_pct
                )
            else:
                table_access_widths_pct[table_name] = access_width_pct

            if table_name not in table_access_method:
                table_access_method[table_name] = [bc.access_op_name]
            else:
                table_access_method[table_name].append(bc.access_op_name)

            if table_name in table_cardinality:
                table_cardinality[table_name] = max(
                    table_cardinality[table_name], bc.cardinality
                )
            else:
                table_cardinality[table_name] = bc.cardinality

        results.append(
            {
                "query_index": qidx,
                "selectivity": table_selectivity,
                "access_width": table_access_widths,
                "access_methods": table_access_method,
                "est_cardinality": table_cardinality,
                "access_width_pct": table_access_widths_pct,
            }
        )

        if qidx % 500 == 0:
            save_checkpoint(results, pathlib.Path("all_queries_features.json"))
            print(
                "Completed index {} of {}".format(qidx, len(queries) - 1),
                file=sys.stderr,
                flush=True,
            )

    save_checkpoint(results, pathlib.Path("all_queries_features.json"))


if __name__ == "__main__":
    main()
