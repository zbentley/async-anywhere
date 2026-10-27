from asyncio import AbstractEventLoop
from contextlib import AbstractContextManager
from copy import copy
from inspect import iscoroutinefunction
from threading import RLock, Lock, Thread
from typing import Iterable, Literal, Callable
from weakref import WeakSet

type RuntimeSelector[T: Thread | AbstractEventLoop] = None | Literal["all"] | T | Iterable[T] | Callable[[], bool]


FAST_COLLECTION_SET_CUTOFF = 5

def validate_coroutine_function(_, attribute, value):
    if not iscoroutinefunction(value):
        raise TypeError(f"{attribute.name} must be a coroutine function (an async def), not {value!r}")

def fast_runtime_checker[T](vals: RuntimeSelector[T], kind: type[T], getter: Callable[[], T]) -> Callable[[], bool]:
    # NB if you pass a callable iterable, this will break.
    if vals is not None:
        if vals == "all":
            return True.__bool__
        elif isinstance(vals, kind):
            vals = WeakSet((vals,))
        elif callable(vals):
            return vals
        else:
            for v in (vals := WeakSet(vals)):
                assert isinstance(v, kind)
        if len(vals) > 0:
            return lambda: getter() in vals
    return bool


def copy_exception[E: BaseException](error: E, *causes: BaseException) -> E:
    """A copy of `error` to raise in its place: the interpreter changes an exception's traceback and context as it's
    raised, as add_note() does its notes, so raising one that others may be raising too would mix theirs into it. The
    copy starts from the original's traceback, and is raised from `causes`, each from the next, if any are given, or else
    from what the original was. Only for exceptions that copy.copy() round-trips: builtins and ours, not users' in general.
    """
    new = copy(error).with_traceback(error.__traceback__)
    new.__context__ = error.__context__
    if len(notes := getattr(error, "__notes__", ())) > 0:
        new.__notes__ = list(notes)
    if len(causes) > 0:
        new.__cause__ = copy_exception(*causes) if len(causes) > 1 else causes[0]  # Copying all but the last, whose cause is kept
    else:
        new.__cause__, new.__suppress_context__ = error.__cause__, error.__suppress_context__
    return new

class LockProtected[T](AbstractContextManager[T]):
    __slots__ = ("_acquire", "_release", "_value")

    def __init__(self, initial: T, recursive=False):
        lock = RLock() if recursive else Lock()
        self._acquire = lock.acquire
        self._release = lock.release
        self._value = initial

    def __exit__(self, *_):
        self._release()

    def __enter__(self):
        self._acquire()
        return self._value
