from collections.abc import Sequence
from typing import TypeVar

_T = TypeVar("_T")
_U = TypeVar("_U")


def to_tuple(arg: Sequence[_T] | _U) -> tuple[_T] | _U:
    if isinstance(arg, Sequence):
        return tuple(arg)
    return arg
