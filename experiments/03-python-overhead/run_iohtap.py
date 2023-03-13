import argparse
import csv
import pathlib
import time
import io

import conductor.lib as cond

from iohtap.config.file import ConfigFile
from iohtap.config.dbtype import DBType
from iohtap.config.routing_policy import RoutingPolicy
from iohtap.config.schema import Schema
from iohtap.server.server import IOHTAPServer


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
    # NOTE: This is unused (just exists for API compatibility)
    strio = io.StringIO()

    try:
        out_dir = cond.get_output_path()
    except RuntimeError:
        out_dir = pathlib.Path()  # The current working directory.

    with IOHTAPServer(config, schema) as server, open(
        out_dir / "results.csv", "w"
    ) as f:
        writer = csv.writer(f)
        writer.writerow(["dbname", "iters", "run_time_ns"])
        for _trial in range(args.trials):
            start = time.time()
            for _iteration in range(args.iters):
                server._handle_request_internal("SELECT 1", strio)
            end = time.time()
            writer.writerow([args.dbname, args.iters, int((end - start) * 1e9)])


if __name__ == "__main__":
    main()
