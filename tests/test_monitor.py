from brad.daemon.monitor import Monitor
from brad.config.file import ConfigFile
import asyncio
from pandas.testing import assert_frame_equal

# pylint: disable=protected-access


def test_read_k_most_recent():
    asyncio.run(f1())


async def f1():
    m = Monitor(ConfigFile("./config/config.yml"))
    task = asyncio.create_task(m.run_forever())

    while m._values.empty:
        await asyncio.sleep(1)

    df = m.read_k_most_recent(3)
    t = m._values.tail(3)
    task.cancel()

    assert df.shape[0] == 3
    assert df.shape[1] == len(m._queries)
    assert_frame_equal(df, t)


def test_read_k_upcoming():
    asyncio.run(f2())


async def f2():
    m = Monitor(ConfigFile("./config/config.yml"))
    task = asyncio.create_task(m.run_forever())

    while m._values.empty:
        await asyncio.sleep(1)

    df = m.read_k_upcoming(5)
    t = m._values.tail(1)
    task.cancel()

    assert df.shape[0] == 5
    assert df.shape[1] == len(m._queries)
    for i in range(5):
        assert_frame_equal(
            df.head(i + 1).tail(1).reset_index(drop=True), t.reset_index(drop=True)
        )


def test_read_upcoming_until():
    asyncio.run(f3())


async def f3():
    m = Monitor(ConfigFile("./config/config.yml"))
    task = asyncio.create_task(m.run_forever())

    while m._values.empty:
        await asyncio.sleep(1)

    end_ts = m._values.index[-1] + 7 * m._epoch_length

    df = m.read_upcoming_until(end_ts)
    t = m._values.tail(1)
    task.cancel()

    assert df.shape[0] == 7
    assert df.shape[1] == len(m._queries)
    for i in range(7):
        assert_frame_equal(
            df.head(i + 1).tail(1).reset_index(drop=True), t.reset_index(drop=True)
        )


def test_read_between_times():
    asyncio.run(f4())


async def f4():
    m = Monitor(ConfigFile("./config/config.yml"))
    task = asyncio.create_task(m.run_forever())

    while m._values.empty:
        await asyncio.sleep(1)

    start_ts = m._values.index[-2]
    end_ts = m._values.index[-1] + 2 * m._epoch_length

    df = m.read_between_times(start_ts, end_ts)
    t = m._values.tail(2)
    task.cancel()

    assert df.shape[0] == 4
    assert df.shape[1] == len(m._queries)
    assert_frame_equal(df.head(2), t)
    for i in range(2):
        assert_frame_equal(
            df.head(i + 3).tail(1).reset_index(drop=True),
            t.tail(1).reset_index(drop=True),
        )


def test_read_between_epochs():
    asyncio.run(f5())


async def f5():
    m = Monitor(ConfigFile("./config/config.yml"))
    task = asyncio.create_task(m.run_forever())

    while m._values.empty:
        await asyncio.sleep(1)

    df = m.read_between_epochs(-2, 1)
    t = m._values.tail(2)
    task.cancel()

    assert df.shape[0] == 4
    assert df.shape[1] == len(m._queries)
    assert_frame_equal(df.head(2), t)
    for i in range(2):
        assert_frame_equal(
            df.head(i + 3).tail(1).reset_index(drop=True),
            t.tail(1).reset_index(drop=True),
        )


def test_reading_cost_metrics():
    asyncio.run(f6())


async def f6():
    m = Monitor(ConfigFile("./config/config.yml"), enable_cost_monitoring=True)
    task = asyncio.create_task(m.run_forever())

    while m._values.empty:
        await asyncio.sleep(1)

    df = m.read_k_most_recent(3)
    t = m._values.tail(3)
    task.cancel()

    assert df.shape[0] == 3
    assert df.shape[1] == len(m._metric_ids)
    assert_frame_equal(df, t)
