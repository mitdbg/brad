from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Generic, TypeVar

from typing_extensions import override

_T = TypeVar("_T")


class ImmutableGenerator(ABC, Generic[_T]):
    """
    Abstract base class for an immutable "generator" (or rather, iterator) that
    yields a (potentially unbounded) sequence of values.

    It implements the `Iterable` protocol, but unlike other iterables in general,
    it can be safely iterated multiple times due to its immutability. This helps
    avoid having to deal with copying (unbounded) iterators or generators.

    Note that this abstract base class offers no mechanism to enforce this property,
    and thus the implementing classes are responsible to satisfy the contract.
    """

    @abstractmethod
    def __iter__(self) -> Iterator[_T]:
        raise NotImplementedError


class SqlGenerator(ImmutableGenerator[str]):
    """
    Abstract base class for an immutable "generator" that yields a
    (potentially unbounded) sequence of SQL strings.
    """

    @override
    @abstractmethod
    def __iter__(self) -> Iterator[str]:
        raise NotImplementedError
