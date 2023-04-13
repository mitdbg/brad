import asyncio
from datetime import timedelta
from typing import AsyncIterator, final, Generator, Optional

from typing_extensions import override
import sys

sys.path.append("..")

from workloads.runner import run_workload
from workloads.runner.client import AsyncClient
from workloads.runner.query import Query
from workloads.runner.reporter import PrintReporter
from workloads.runner.schedule import Once, Repeat
from workloads.runner.time import get_current_time
from workloads.runner.user import User
from workloads.runner.workload import Workload
from brad.async_grpc_client import AsyncBradGrpcClient
from brad.config.session import SessionId


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
            print(tup)
            yield tup


async def run_IMDB() -> None:
    current_time = get_current_time()
    interval = timedelta(seconds=1)

    workload = Workload.combine(
        [
            Workload.serial(
                [
                    Query(
                        f"SELECT COUNT(*) FROM info_type WHERE id > {i};",
                        Once(at=current_time + 0.47 * i * interval),
                    )
                    for i in range(10)
                ],
                user=User.with_label("Once"),
            ),
            Workload.serial(
                [
                    Query(
                        """SELECT MAX("title"."episode_nr" + "movie_companies"."movie_id") 
                           as agg_0 FROM "company_type" LEFT OUTER JOIN "movie_companies" 
                           ON "company_type"."id" = "movie_companies"."company_type_id" LEFT OUTER JOIN "title" 
                           ON "movie_companies"."movie_id" = "title"."id" LEFT OUTER JOIN "company_name" ON 
                           "movie_companies"."company_id" = "company_name"."id"  WHERE "title"."title" NOT LIKE '%t%he%' 
                           AND "movie_companies"."note" NOT LIKE '%media)%' AND ("company_type"."kind" NOT LIKE 
                           '%companie%s%' OR "company_type"."id" 
                           BETWEEN 2 AND 3 OR "company_type"."kind" LIKE '%companies%') AND 
                           "company_name"."country_code" NOT LIKE '%[us]%';""",
                        Repeat.starting_now(
                            interval=timedelta(seconds=20), num_repeat=5
                        ),
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
