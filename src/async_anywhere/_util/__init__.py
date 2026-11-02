import asyncio
from copy import copy
from inspect import iscoroutinefunction
from typing import Iterable, Literal

type LoopSelector = None | Literal["all"] | Iterable[asyncio.AbstractEventLoop]

def validate_coroutine_function(_, attribute, value):
    if not iscoroutinefunction(value):
        raise TypeError(f"{attribute.name} must be a coroutine function (an async def), not {value!r}")


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
