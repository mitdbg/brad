import argparse
import asyncio
import csv
import pathlib
import time

import conductor.lib as cond

from brad.config.file import ConfigFile
from brad.config.engine import Engine
from brad.front_end.front_end import BradFrontEnd
from brad.routing.policy import RoutingPolicy


class ConfigFileOverride(ConfigFile):
    def __init__(self, path: str, engine: Engine):
        super().__init__(path)
        if engine == Engine.Athena:
            self._policy = RoutingPolicy.AlwaysAthena
        elif engine == Engine.Aurora:
            self._policy = RoutingPolicy.AlwaysAurora
        elif engine == Engine.Redshift:
            self._policy = RoutingPolicy.AlwaysRedshift
        else:
            raise RuntimeError

    @property
    def routing_policy(self) -> str:
        return self._policy


async def run_brad_experiment(args, brad: BradFrontEnd, out_dir: pathlib.Path):
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
    parser.add_argument("--config-file", type=str, required=True)
    parser.add_argument("--planner-config", type=str, required=True)
    parser.add_argument("--schema-name", type=str, required=True)
    parser.add_argument("--engine", type=str, required=True)
    parser.add_argument("--iters", type=int, default=50)
    parser.add_argument("--trials", type=int, default=5)
    args = parser.parse_args()

    engine = Engine.from_str(args.engine)
    config = ConfigFileOverride(args.config_file, engine)

    try:
        out_dir = cond.get_output_path()
    except RuntimeError:
        out_dir = pathlib.Path()  # The current working directory.

    brad = BradFrontEnd(config, args.schema_name, args.planner_config, debug_mode=False)
    asyncio.run(run_brad_experiment(args, brad, out_dir))


if __name__ == "__main__":
    main()
