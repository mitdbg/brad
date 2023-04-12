"""
NOTE: No need to handle cancellation here.
"""

import asyncio
from asyncio import PriorityQueue, Queue, Task
from typing import Any, Final

from brad.runner.client import AsyncClient
from brad.runner.query import CompletedQuery, PendingQuery, Queries, Query, User
from brad.runner.reporter import QueryReporter
from brad.runner.time import get_current_time, get_event_loop_time, measure_time
from brad.runner.workload import Workload

_MAX_PENDING_QUEUE_SIZE: Final[int] = 10


async def run_workload(
    workload: Workload, client: AsyncClient[Any], reporter: QueryReporter
) -> None:
    """
    Run the workload asynchronously with the given client.
    `client` must already be connected.
    """
    background_tasks = set[Task[None]]()
    completed_queue = Queue[CompletedQuery]()

    for user, queries in workload:
        pending_queue = PriorityQueue[tuple[float, PendingQuery]](
            maxsize=_MAX_PENDING_QUEUE_SIZE
        )

        # Keep references to the background tasks
        # See https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
        background_tasks |= {
            asyncio.create_task(
                _initial_scheduler_worker(user, queries, pending_queue)
            ),
            asyncio.create_task(
                _executor_worker(pending_queue, completed_queue, client)
            ),
        }

    # Ensure the removal of the saved references
    for task in background_tasks:
        task.add_done_callback(background_tasks.discard)

    await _reporter_worker(completed_queue, reporter)


async def _process_single_query(
    user: User, query: Query, pending_queue: PriorityQueue[tuple[float, PendingQuery]]
) -> None:
    processed_time = get_current_time()

    # Use the event loop time for scheduling purposes
    # since it guarantees monotonicity
    scheduled_time = (
        get_event_loop_time() + query.schedule.time_until_next().total_seconds()
    )

    await pending_queue.put(
        (
            scheduled_time,
            PendingQuery(user=user, query=query, processed_time=processed_time),
        )
    )


async def _initial_scheduler_worker(
    user: User,
    queries: Queries,
    pending_queue: PriorityQueue[tuple[float, PendingQuery]],
) -> None:
    for query in queries:
        await _process_single_query(user, query, pending_queue)


async def _executor_worker(
    pending_queue: PriorityQueue[tuple[float, PendingQuery]],
    completed_queue: Queue[CompletedQuery],
    client: AsyncClient[Any],
) -> None:
    while True:
        scheduled_time, pending_query = await pending_queue.get()

        # Check the scheduled against the event loop time
        current_event_loop_time = get_event_loop_time()
        if current_event_loop_time < scheduled_time:
            await asyncio.sleep(scheduled_time - current_event_loop_time)

        with measure_time() as measurement:
            result = [row async for row in client.execute(pending_query.query.sql)]

        await completed_queue.put(
            pending_query.mark_complete(
                result=result,
                executed_time=measurement.start_time,
                execution_time=measurement.elapsed_time,
            )
        )

        pending_queue.task_done()

        # Check for repeats
        # TODO: Confirm correctness
        if pending_query.query.schedule.time_until_next().total_seconds() > 0:
            await _process_single_query(
                pending_query.user, pending_query.query, pending_queue
            )


async def _reporter_worker(
    completed_queue: Queue[CompletedQuery], reporter: QueryReporter
) -> None:
    while True:
        completed_query = await completed_queue.get()
        await reporter.report(completed_query)
        completed_queue.task_done()
        # TODO: How to signal end
