import asyncio
import logging
import signal
from copy import copy
from functools import cache
from inspect import iscoroutinefunction
from threading import Lock, RLock, current_thread, main_thread
from typing import Callable, ClassVar, Iterable, Literal, Sequence, final

type LoopSelector = None | Literal["all"] | Iterable[asyncio.AbstractEventLoop]

def validate_coroutine_function(_, attribute, value):
    if not iscoroutinefunction(value):
        raise TypeError(f"{attribute.name} must be a coroutine function (an async def), not {value!r}")


@cache
def futures_logger() -> logging.Logger:
    """What async_anywhere's futures and state trackers log to (their done callbacks' errors, say): a child, by name, of
    concurrent.futures' logger, where concurrent.futures.Future logs its callbacks' errors, so that what's configured
    there applies here too. Made on first use, so that it hooks into logging's configuration as late as it can."""
    import concurrent.futures._base
    # TODO guard against the symbol moving?
    return concurrent.futures._base.LOGGER.getChild("async_anywhere")


class BlockingSignals[**P, T]:
    """fn, called with the given signals (numbers, or names) blocked while it runs on the main thread, the only one
    Python runs signal handlers on: one that arrives meanwhile is raised again once fn returns, so that its handler
    runs then, as though it had arrived then. They're raised in the order given, each handler running whatever the one
    before raised, as CPython runs pending signals' (see _raise_each()). A handler fn installs for one is kept. Equal
    to fn, so that remove_done_callback(fn) finds it. partial(BlockingSignals, signals) decorates."""

    __slots__ = ("_signals", "_fn")
    _UNBLOCKABLE: ClassVar[frozenset[int]] = frozenset(
        getattr(signal, name)
        for name in ("SIGKILL", "SIGSTOP", "CTRL_C_EVENT", "CTRL_BREAK_EVENT")
        if hasattr(signal, name)
    )

    def __init__(self, signals: Sequence[int | str], fn: Callable[P, T]):
        signums = {}
        for value in signals:
            try:
                signum = signal.Signals[value] if isinstance(value, str) else signal.Signals(value)
            except (KeyError, TypeError, ValueError):
                signum = None
            if signum is None or signum in self._UNBLOCKABLE:
                raise ValueError(f"Can't block signal {value!r}")
            signums[signum] = None
        self._signals, self._fn = tuple(signums), fn

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        if current_thread() is not main_thread():
            return self._fn(*args, **kwargs)
        arrived, blocked = set(), []

        def record(signum, _):
            arrived.add(signum)

        try:
            for signum in self._signals:
                if signal.getsignal(signum) is not None:  # Else installed outside Python, so it couldn't be put back
                    blocked.append((signum, signal.signal(signum, record)))
            return self._fn(*args, **kwargs)
        finally:
            for signum, handler in blocked:
                if (current := signal.signal(signum, handler)) is not record:  # Installed by fn: so kept
                    signal.signal(signum, current)
            _raise_each([signum for signum, _ in blocked if signum in arrived])

    def __eq__(self, other):
        return self._fn == other

    def __hash__(self):
        return hash(self._fn)


def _raise_each(signums: list[int]):
    """Raises each signal in turn, each in a finally block after the one before: so each one's handler runs whatever the
    one before raised, and what it raises has that as its context, as with CPython's own pending signals."""
    if len(signums) > 0:
        try:
            signal.raise_signal(signums[0])
        finally:
            _raise_each(signums[1:])


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


@final
class LockProtected[T]:
    """A value reachable only under its lock: `with protected as value:` holds the lock while the block runs."""
    __slots__ = ("_acquire", "_release", "_value")

    def __init__(self, value: T, recursive: bool = False):
        lock = RLock() if recursive else Lock()
        # Bound once: calling these is cheaper than looking up the lock's methods per call
        self._acquire, self._release = lock.acquire, lock.release
        self._value = value

    def __enter__(self) -> T:
        self._acquire()
        return self._value

    def __exit__(self, exc_type, exc, tb):  # Not *args: a fixed arity is called without packing an args tuple
        self._release()

    def replace_value(self, value: T):
        """Replaces the value that entering returns. Must be called under the lock (in a `with` block on this)."""
        self._value = value
