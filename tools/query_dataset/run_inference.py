import sys
import argparse
import asyncio
import numpy as np
import pathlib

from typing import List
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.connection.factory import ConnectionFactory

# HACK: Unfortunately this is needed because the GNN's dependencies include
# out-of-source modules.
sys.path.append(str(pathlib.Path(__file__).parents[2]))
from brad.cost_model.inference import TrainedModel


def load_queries(file_name: str) -> List[str]:
    with open(file_name, "r", encoding="UTF-8") as file:
        return [line.strip() for line in file]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", type=str, required=True)
    parser.add_argument("--model-file", type=str, required=True)
    parser.add_argument("--model-stats-file", type=str, required=True)
    parser.add_argument("--database-stats-file", type=str, required=True)
    parser.add_argument("--queries-file", type=str, required=True)
    parser.add_argument("--config-file", type=str, required=True)
    parser.add_argument("--schema-name", type=str, required=True)
    parser.add_argument("--out-file", type=str, required=True)
    parser.add_argument("--undo-log", action="store_true")
    parser.add_argument("--undo-mega", action="store_true")
    args = parser.parse_args()

    engine = Engine.from_str(args.engine)
    queries = load_queries(args.queries_file)
    config = ConfigFile.load(args.config_file)
    conn = asyncio.run(ConnectionFactory.connect_to_sidecar(args.schema_name, config))

    model = TrainedModel.load(
        engine, args.model_file, args.model_stats_file, args.database_stats_file
    )
    predictions = model.predict(queries, conn)

    if args.undo_log and args.undo_mega:
        print("WARNING: Both --undo-log and --undo-mega used.")

    if args.undo_log:
        print("Undoing natural log...")
        predictions = np.exp(predictions)

    if args.undo_mega:
        print("Undoing the mega conversion...")
        predictions = predictions * 1e6

    np.save(args.out_file, predictions)
    print("Done!")


if __name__ == "__main__":
    main()
