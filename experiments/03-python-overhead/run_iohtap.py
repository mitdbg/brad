import argparse
import asyncio
import csv
import pathlib
import time

import conductor.lib as cond

from brad.config.file import ConfigFile
from brad.config.dbtype import DBType
from brad.config.routing_policy import RoutingPolicy
from brad.config.schema import Schema
from brad.front_end.server import BradServer


class ConfigFileOverride(ConfigFile):
    def __init__(self, path: str, dbname: str):
        super().__init__(path)
        if dbname == DBType.Athena:
            self._policy = RoutingPolicy.AlwaysAthena
        elif dbname == DBType.Aurora:
            self._policy = RoutingPolicy.AlwaysAurora
        elif dbname == DBType.Redshift:
            self._policy = RoutingPolicy.AlwaysRedshift
        else:
            raise RuntimeError

    @property
    def routing_policy(self) -> str:
        return self._policy


async def run_brad_experiment(args, brad: BradServer, out_dir: pathlib.Path):
    await brad.run_setup()
    session_id = await brad.start_session()
    try:
        with open(out_dir / "results.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(["dbname", "iters", "run_time_ns"])
            for _trial in range(args.trials):
                start = time.time()
                for _iteration in range(args.iters):
                    async for _row in brad.run_query(session_id, "SELECT 1"):
                        pass
                end = time.time()
                writer.writerow([args.dbname, args.iters, int((end - start) * 1e9)])
    finally:
        await brad.end_session(session_id)
        await brad.run_teardown()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dbname", type=str, required=True)
    parser.add_argument("--config-file", type=str, required=True)
    parser.add_argument("--schema-file", type=str, required=True)
    parser.add_argument("--iters", type=int, default=50)
    parser.add_argument("--trials", type=int, default=5)
    args = parser.parse_args()

    config = ConfigFileOverride(args.config_file, args.dbname)
    schema = Schema.load(args.schema_file)

    try:
        out_dir = cond.get_output_path()
    except RuntimeError:
        out_dir = pathlib.Path()  # The current working directory.

    brad = BradServer(config, schema)
    asyncio.run(run_brad_experiment(args, brad, out_dir))


if __name__ == "__main__":
    main()
