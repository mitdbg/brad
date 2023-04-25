import asyncio
from typing import AsyncIterator, Optional
from absl import app
from absl import flags

from typing_extensions import override
import sys
sys.path.append("..")

from workloads.runner import run_workload
from workloads.runner.client import AsyncClient
from workloads.runner.reporter import PrintReporter
from workloads.runner.read_workload import make_imdb_workload, read_test_run_workload
import workloads.IMDB.parameters as parameters
from brad.async_grpc_client import AsyncBradGrpcClient
from brad.config.session import SessionId


FLAGS = flags.FLAGS
flags.DEFINE_string("run", "test", "Experiment config to run.")

class BradClient(AsyncClient[str]):
    def __init__(self, host: str, port: int):
        self._impl = AsyncBradGrpcClient(host, port)
        self._session_id: Optional[SessionId] = None

    @override
    async def connect(self) -> None:
        self._impl.connect()
        self._session_id = await self._impl.start_session()

    @override
    async def close(self) -> None:
        self._impl.close()
        self._session_id = None

    @override
    async def execute(self, query: str) -> AsyncIterator[str]:
        res = self._impl.run_query(self._session_id, query)
        async for tup in res:
            yield tup


async def run_IMDB(param: parameters.Params) -> None:
    p = param
    if p.test_run:
        workload = read_test_run_workload()
    else:
        workload = make_imdb_workload(p.txn_query_dir, p.analytic_query_dir, p.total_num_txn_users,
                                      p.total_num_analytic_users, p.reporting_time_window)

    reporter = PrintReporter()

    # Todo: using multi-threading to speed up
    async with BradClient(host="0.0.0.0", port=6583) as client:
        await run_workload(workload, client, reporter)


def Main(argv):
    del argv  # Unused.
    name = FLAGS.run
    print("Looking up params by name:", name)
    p = parameters.Get(name)
    asyncio.run(run_IMDB(p))


if __name__ == "__main__":
    app.run(Main)
