import asyncio
from copy import copy
from inspect import iscoroutinefunction
from typing import Any, Iterable, Literal, Callable
from weakref import WeakSet

type LoopSelector = None | Literal["all"] | Iterable[asyncio.AbstractEventLoop]

def validate_coroutine_function(_, attribute, value):
    if not iscoroutinefunction(value):
        raise TypeError(f"{attribute.name} must be a coroutine function (an async def), not {value!r}")

def blocking_wait_checker(loops: LoopSelector) -> Callable[[Any], None] | None:
    """A function that raises RuntimeError, naming the future it's given, if called in one of `loops` (any, for "all")
    while it runs: or None, if there are none to check. It holds the loops weakly."""
    if loops == "all":
        refuse = lambda loop: loop is not None  # None is outside any loop
    elif len(refs := WeakSet(loops)) > 0:
        assert all(isinstance(loop, asyncio.AbstractEventLoop) for loop in refs)
        refuse = refs.__contains__  # None, outside a loop, is never in refs
    else:
        return None

    def check(future):
        if refuse(loop := asyncio._get_running_loop()):
            raise RuntimeError(
                f"Waiting for {future!r} would deadlock: current event loop '{loop!r}' was set in 'prevent_blocking_wait_in_loops'")

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
