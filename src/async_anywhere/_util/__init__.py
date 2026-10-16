from contextlib import AbstractContextManager
from inspect import iscoroutinefunction
from threading import RLock, Lock
from typing import TYPE_CHECKING, Iterable, Collection

if TYPE_CHECKING:  # async_anywhere.futures imports this module
    pass

FAST_COLLECTION_SET_CUTOFF = 5

def validate_coroutine_function(_, attribute, value):
    if not iscoroutinefunction(value):
        raise TypeError(f"{attribute.name} must be a coroutine function (an async def), not {value!r}")

def fast_collection[T](items: Iterable[T]) -> Collection[T]:
    items = tuple(items)
    return frozenset(items) if len(items) > FAST_COLLECTION_SET_CUTOFF else items

class LockProtected[T](AbstractContextManager[T]):
    __slots__ = ("_lock", "_value")

    def __init__(self, initial: T, recursive=False):
        self._lock = RLock() if recursive else Lock()
        self._value = initial

    def __exit__(self, *_):
        self._lock.release()

    def __enter__(self):
        self._lock.acquire()
        return self._value
