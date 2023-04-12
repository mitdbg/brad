import asyncio
from datetime import timedelta
from typing import AsyncIterator, final, Generator, Optional

from typing_extensions import override

from workloads.runner import run_workload
from workloads.runner.client import AsyncClient
from workloads.runner.query import Query
from workloads.runner.reporter import PrintReporter
from workloads.runner.schedule import Once, Repeat
from workloads.runner.time import get_current_time
from workloads.runner.user import User
from workloads.runner.workload import Workload
from brad.grpc_client import BradGrpcClient
from brad.config.session import SessionId

class BradClient(AsyncClient[str]):
    def __init__(self, host: str, port: int):
        self._impl = BradGrpcClient(host, port)
        self._session_id: Optional[SessionId] = None
    @override
    async def connect(self) -> None:
        self._impl.connect()
        self._session_id = self._impl._session_id

    @override
    async def close(self) -> None:
        self._impl.close()
        self._session_id = None

    @override
    async def execute(self, query: str) -> AsyncIterator[str]:
        res = self._impl.run_query(query)
        for tup in res:
            yield tup


async def run_IMDB() -> None:

    current_time = get_current_time()
    interval = timedelta(seconds=1)

    workload = Workload.combine(
        [
            Workload.serial(
                [
                    Query(f"SELECT COUNT(*) FROM info_type WHERE id > {i};", Once(at=current_time + 0.47 * i * interval))
                    for i in range(10)
                ],
                user=User.with_label("Once"),
            ),
            Workload.serial(
                [
                    Query(
                        "SELECT COUNT(*) FROM title;",
                        Repeat.starting_now(interval=interval, num_repeat=20),
                    ),
                ],
                user=User.with_label("Repeat"),
            ),
        ]
    )

    reporter = PrintReporter()

    async with BradClient(host="0.0.0.0", port=6583) as client:
        await run_workload(workload, client, reporter)


if __name__ == "__main__":
    asyncio.run(run_IMDB())
