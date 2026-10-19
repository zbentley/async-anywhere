from contextlib import AbstractContextManager
from copy import copy
from inspect import iscoroutinefunction
from threading import RLock, Lock
from typing import Iterable, Collection


FAST_COLLECTION_SET_CUTOFF = 5


def validate_coroutine_function(_, attribute, value):
    if not iscoroutinefunction(value):
        raise TypeError(f"{attribute.name} must be a coroutine function (an async def), not {value!r}")

def fast_collection[T](items: Iterable[T]) -> Collection[T]:
    items = tuple(items)
    return frozenset(items) if len(items) > FAST_COLLECTION_SET_CUTOFF else items

def copy_exception[E: BaseException](error: E, *causes: BaseException) -> E:
    """A copy of `error` to raise in its place: the interpreter changes an exception's traceback and context as it's
    raised, as add_note() does its notes, so raising one that others may be raising too would mix theirs into it. The
    copy starts from the original's traceback, and is raised from `causes`, each from the next, if any are given, or else
    from what the original was. Only for exceptions that copy.copy() round-trips: builtins and ours, not users' in general.
    """
    new = copy(error).with_traceback(error.__traceback__)
    new.__context__ = error.__context__
    if notes := getattr(error, "__notes__", ()):
        new.__notes__ = list(notes)
    if causes:
        new.__cause__ = copy_exception(*causes) if causes[1:] else causes[0]  # Copying all but the last, whose cause is kept
    else:
        new.__cause__, new.__suppress_context__ = error.__cause__, error.__suppress_context__
    return new

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
