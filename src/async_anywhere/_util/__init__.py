from asyncio import AbstractEventLoop, _get_running_loop
from contextlib import AbstractContextManager
from copy import copy
from inspect import iscoroutinefunction
from threading import RLock, Lock, Thread, current_thread
from typing import Any, Iterable, Literal, Callable
from weakref import WeakSet

type RuntimeSelector[T] = None | Literal["all"] | T | Iterable[T] | Callable[[], bool]
type ThreadSelector = RuntimeSelector[Thread]
type LoopSelector = RuntimeSelector[AbstractEventLoop]

FAST_COLLECTION_SET_CUTOFF = 5

def validate_coroutine_function(_, attribute, value):
    if not iscoroutinefunction(value):
        raise TypeError(f"{attribute.name} must be a coroutine function (an async def), not {value!r}")

def blocking_wait_checker(loops: LoopSelector, threads: ThreadSelector) -> Callable[[Any], None] | None:
    """A function that raises RuntimeError, naming the future it's given, if called on one of `threads`, or in one of
    `loops` while it runs: or None, if there are none to check. It holds the loops and threads weakly."""
    refs, tests = WeakSet(), {AbstractEventLoop: bool, Thread: bool}  # tests: for "all", and callables given instead
    for kind, vals in ((AbstractEventLoop, loops), (Thread, threads)):
        # NB if you pass a callable iterable, this will break.
        if vals == "all":
            tests[kind] = True.__bool__
        elif callable(vals):
            tests[kind] = vals
        elif vals is not None:
            for v in (vals,) if isinstance(vals, kind) else vals:
                assert isinstance(v, kind)
                refs.add(v)
    in_loop, in_thread = tests[AbstractEventLoop], tests[Thread]
    if len(refs) == 0 and in_loop is in_thread is bool:
        return None

    def check(future):
        # None, outside a loop, is never in refs
        if (loop := _get_running_loop()) in refs or in_loop():
            raise RuntimeError(
                f"Waiting for {future!r} would deadlock: current event loop '{loop!r}' was set in 'prevent_blocking_wait_in_loops'")
        if (thread := current_thread()) in refs or in_thread():
            raise RuntimeError(
                f"Waiting for {future!r} would deadlock: current thread '{thread!r}' was set in 'prevent_blocking_wait_in_threads'")

    return check


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
