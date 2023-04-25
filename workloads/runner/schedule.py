from __future__ import annotations

import itertools
from abc import abstractmethod
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Protocol, final, overload
from typing_extensions import override

from attrs import define

from workloads.runner.generator import ImmutableGenerator
from workloads.runner.time import get_current_time


class Schedule(Protocol):
    """
    Represents a schedule for executing some event.

    Conceptually similar to a cron expression, but designed
    to be simpler for typical database workloads.
    """

    @abstractmethod
    def time_until_next(self) -> timedelta:
        """
        Returns the remaining time in seconds until the next event where
        the schedule is fired, or zero if the schedule has no upcoming event
        in the future (which means, present time also returns zero).

        Note that the returned `timedelta` must be nonnegative.
        """
        raise NotImplementedError


@final
@define(frozen=True)
class Once(Schedule):
    """
    Schedules an event at one particular time.
    """

    at: datetime

    @override
    def time_until_next(self) -> timedelta:
        return max(self.at - get_current_time(), timedelta(0))

    @staticmethod
    def now() -> Once:
        """
        Returns a `Once` schedule at the current time.

        Note that this means future calls to `time_until_next()`
        will definitely return zero (assuming monotonic clock).
        """
        return Once(at=get_current_time())


@final
@define(frozen=True, kw_only=True)
class Repeat(Schedule):
    """
    Schedules an event to repeat every `interval` period of time,
    starting from `start_time`, ending at `end_time`.
    """

    interval: timedelta
    start_time: datetime
    end_time: datetime = datetime.max

    @override
    def time_until_next(self) -> timedelta:
        current_time = get_current_time()

        # Case 1: Schedule has not started
        if current_time <= self.start_time:
            return self.start_time - current_time

        # Case 2: Schedule is fully completed
        if current_time > self.end_time:
            return timedelta(0)

        # Case 3: Schedule in progress
        elapsed_time = current_time - self.start_time
        time_since_last_event = elapsed_time % self.interval
        time_until_next_event = self.interval - time_since_last_event

        return min(time_until_next_event, self.end_time - current_time)

    @overload
    @staticmethod
    def starting_now(*, interval: timedelta, num_repeat: int) -> Repeat:
        ...

    @overload
    @staticmethod
    def starting_now(
        *, interval: timedelta, end_time: datetime = datetime.max
    ) -> Repeat:
        ...

    @staticmethod
    def starting_now(
        *,
        interval: timedelta,
        end_time: datetime = datetime.max,
        num_repeat: int | None = None,
    ) -> Repeat:
        """
        Returns a `Repeat` schedule that starts at the current time,
        and ends either at `end_time` or after `num_repeat` repeats.
        """
        start_time = get_current_time()

        # First overload: `num_repeat`
        if num_repeat is not None:
            if num_repeat < 1:
                raise ValueError("`num_repeat` must be at least 1")

            return Repeat(
                interval=interval,
                start_time=start_time,
                end_time=start_time + num_repeat * interval,
            )

        # Second overload: `end_time`
        assert end_time is not None
        return Repeat(
            interval=interval,
            start_time=start_time,
            end_time=end_time,
        )

    @staticmethod
    def starting_at(
            *,
            start_time: datetime,
            interval: timedelta,
            end_time: datetime = datetime.max,
            num_repeat: int | None = None,
    ) -> Repeat:
        """
        Returns a `Repeat` schedule that starts at the current time,
        and ends either at `end_time` or after `num_repeat` repeats.
        """
        if start_time is None:
            start_time = get_current_time()

        # First overload: `num_repeat`
        if num_repeat is not None:
            if num_repeat < 1:
                raise ValueError("`num_repeat` must be at least 1")

            return Repeat(
                interval=interval,
                start_time=start_time,
                end_time=start_time + num_repeat * interval,
            )

        # Second overload: `end_time`
        assert end_time is not None
        return Repeat(
            interval=interval,
            start_time=start_time,
            end_time=end_time,
        )


class ScheduleGenerator(ImmutableGenerator[Schedule]):
    """
    Abstract base class for an immutable "generator" that yields
    a (potentially unbounded) sequence of schedules.

    Note that the two default implementing classes are made private,
    but they can be constructed via the factory methods on this class.
    This is a matter of preference; I think this improves readability.
    """

    @override
    @abstractmethod
    def __iter__(self) -> Iterator[Schedule]:
        raise NotImplementedError

    @staticmethod
    def fixed(*, schedule: Schedule, max_items: int | None) -> ScheduleGenerator:
        """
        Returns a generator that always yields the given `schedule` up to
        `max_items` times (or unbounded if it is `None`).
        """
        return _FixedScheduleGenerator(schedule=schedule, max_items=max_items)

    @staticmethod
    def evenly_spaced(
        *,
        interval: timedelta,
        start_time: datetime | None = None,
        end_time: datetime = datetime.max,
        max_items: int | None = None,
    ) -> ScheduleGenerator:
        """
        Returns a generator that yields consecutive `Once` schedules
        at evenly-spaced interval up to `max_items` times (or unbounded
        if it is `None`), starting from `start_time` to `end_time`.

        If `start_time` is `None`, set it to the current time at construction.
        """
        return _EvenlySpacedScheduleGenerator(
            interval=interval,
            start_time=(start_time if start_time is not None else get_current_time()),
            end_time=end_time,
            max_items=max_items,
        )


@final
@define(frozen=True)
class _FixedScheduleGenerator(ScheduleGenerator):
    """
    Schedule generator implementation that yields a fixed schedule.
    """

    schedule: Schedule
    max_items: int | None

    @override
    def __iter__(self) -> Iterator[Schedule]:
        if self.max_items is None:
            while True:
                yield self.schedule
        else:
            for _ in range(self.max_items):
                yield self.schedule


@final
@define(frozen=True)
class _EvenlySpacedScheduleGenerator(ScheduleGenerator):
    """
    Schedule generator implementation that yields evenly-spaced schedules.
    """

    interval: timedelta
    start_time: datetime
    end_time: datetime
    max_items: int | None

    @override
    def __iter__(self) -> Iterator[Schedule]:
        count_range = (
            range(self.max_items) if self.max_items is not None else itertools.count()
        )
        for i in count_range:
            yield Once(at=self.start_time + i * self.interval)
