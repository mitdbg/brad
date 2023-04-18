import asyncio
import sys
from datetime import timedelta
from typing import AsyncIterator, final
from typing_extensions import override

sys.path.append("..")

from workloads.runner import run_workload
from workloads.runner.client import AsyncClient
from workloads.runner.query import Query
from workloads.runner.reporter import PrintReporter
from workloads.runner.schedule import Once, Repeat
from workloads.runner.time import get_current_time
from workloads.runner.user import User
from workloads.runner.workload import Workload


@final
class NoopClient(AsyncClient[str]):
    @override
    async def connect(self) -> None:
        return

    @override
    async def close(self) -> None:
        return

    @override
    async def execute(self, query: str) -> AsyncIterator[str]:
        yield query


@final
class BradClient(AsyncClient[str]):
    @override
    async def connect(self) -> None:
        return

    @override
    async def close(self) -> None:
        return

    @override
    async def execute(self, query: str) -> AsyncIterator[str]:
        yield query


async def main() -> None:
    current_time = get_current_time()
    interval = timedelta(seconds=1)

    workload = Workload.combine(
        [
            Workload.serial(
                [
                    Query(f"SELECT {i};", Once(at=current_time + 0.47 * i * interval))
                    for i in range(10)
                ],
                user=User.with_label("Once"),
            ),
            Workload.serial(
                [
                    Query(
                        "SELECT *;",
                        Repeat.starting_now(interval=interval, num_repeat=20),
                    ),
                ],
                user=User.with_label("Repeat"),
            ),
        ]
    )

    reporter = PrintReporter()

    async with NoopClient() as client:
        await run_workload(workload, client, reporter)


asyncio.run(main())
