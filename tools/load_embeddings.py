import asyncio
import argparse
import numpy as np
import numpy.typing as npt

from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.connection.connection import Connection
from brad.connection.factory import ConnectionFactory
from brad.asset_manager import AssetManager


# Killed if I insert data in 1 batch
BATCH_SIZE = 50_000


def insert(connection: Connection, embeddings: npt.NDArray):
    cursor = connection.cursor_sync()

    # Get the ids.
    cursor.execute_sync("SELECT DISTINCT id FROM aka_title")
    movie_id_rows = cursor.fetchall_sync()
    all_movie_ids = [row[0] for row in movie_id_rows]

    total_batches = embeddings.shape[0] // BATCH_SIZE
    if embeddings.shape[0] % BATCH_SIZE != 0:
        total_batches += 1

    # Insert batches
    batch = 0
    while batch * BATCH_SIZE < embeddings.shape[0]:
        np_embeddings_batch = embeddings[batch * BATCH_SIZE : (batch + 1) * BATCH_SIZE]
        movie_ids_batch = all_movie_ids[batch * BATCH_SIZE : (batch + 1) * BATCH_SIZE]

        insert_batch = [
            (
                id,
                str(list(e)),
            )
            for id, e in zip(movie_ids_batch, np_embeddings_batch)
        ]

        print(f"Loading batch {batch} of {total_batches}...")
        cursor.executemany_sync(
            "INSERT INTO embeddings (movie_id, embedding) VALUES (?,?);", insert_batch
        )

        batch += 1

    cursor.commit_sync()


def inspect(connection: Connection):
    cursor = connection.cursor_sync()
    cursor.execute_sync("SELECT MAX(id) FROM embeddings;")

    l = cursor.fetchall_sync()
    for li in l:
        print(li)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", type=str, required=True)
    parser.add_argument("--schema-name", type=str, required=True)
    parser.add_argument("--embeddings-file", type=str)
    parser.add_argument("--load", action="store_true")
    args = parser.parse_args()

    if args.load:
        embeddings = np.load(args.embeddings_file)
    else:
        embeddings = None

    config = ConfigFile.load(args.config_File)
    assets = AssetManager(config)
    blueprint_mgr = BlueprintManager(config, assets, args.schema_name)
    asyncio.run(blueprint_mgr.load())
    aurora = ConnectionFactory.connect_to_sync(
        Engine.Aurora,
        args.schema_name,
        config,
        blueprint_mgr.get_directory(),
        autocommit=False,
    )

    if args.load:
        insert(aurora, embeddings)
    inspect(aurora)

    aurora.close_sync()


if __name__ == "__main__":
    main()
