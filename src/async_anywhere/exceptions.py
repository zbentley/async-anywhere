r"""
Exceptions raised by :py:mod:`async_anywhere`

Exception classes in this file all have the following behavior:

- All custom (non-stdlib) exceptions raised by this library are subclasses of :py:class:`AsyncAnywhereException`.
- All :py:class:`AsyncAnywhereException`\ s can be pickled/unpickled/copied and shared between processes.
- Code in this library that raises exceptions (including stdlib Exceptions and :py:class:`AsyncAnywhereException`\ s)
  does so in a way that does not artificially prolong object lifetimes (e.g. this library will not raise errors whose
  handling or storage results in an event loop not being garbage collected when it should be). If you find that this is
  not the case in a specific situation, file a bug.

Users interacting with exception classes defined in this module must follow these rules:

- Do not subclass or manually construct :py:class:`AsyncAnywhereException` or any subclass thereof from user code.
- Treat instances of :py:class:`AsyncAnywhereException` as immutable. Changing any fields on them may break functionality.
-
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import copyreg
import os
from abc import ABC
from functools import partial
from typing import TYPE_CHECKING
from weakref import WeakSet

import async_anywhere

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any, Self

    from async_anywhere._util import LoopSelector
    from async_anywhere.state_tracker import State, StateTracker


__all__ = [
    "AsyncAnywhereException",
    "CleanStop",
    "StateTrackerError",
    "WorkerBroken",
    "WorkerForked",
    "WorkerInterrupted",
    "WorkerStopped",
    "WouldBlock",
]


# ABC in a class's own bases marks one we never instantiate: only a marker, as BaseException.__new__() checks nothing
class AsyncAnywhereException(ABC):
    """Mixed into every exception async_anywhere raises of its own: not an Exception itself, so that each can extend
    whichever builtin exception fits it."""

    __slots__ = ()

    def __reduce__(self):
        # For copy and pickle: recreate from args (for some, a formatted message) without calling __init__, which may
        # not take them
        return copyreg.__newobj__, (type(self), *self.args), vars(self)

    def copy(self, *causes: BaseException) -> Self:
        """A copy to raise in this one's place, raised from `causes` if any are given: see _util.copy_exception()."""
        from async_anywhere._util import copy_exception  # Only on the sad paths that copy: see the module's docstring

        return copy_exception(self, *causes)


# Futures


class WouldBlock(AsyncAnywhereException, RuntimeError):
    def __init__(self, future, loop) -> None:
        super().__init__(f"Waiting for {future!r} would deadlock on current event loop '{loop!r}'")

    @classmethod
    def _invariant(cls, would_block, future):
        if would_block(loop := asyncio._get_running_loop()):
            raise cls(future, loop)

    @classmethod
    def _checker(cls, loops: LoopSelector) -> Callable[[Any], None] | None:
        """A function that raises WouldBlock, naming the future it's given, if called in one of `loops` (any, for "all")
        while it runs: or None, if there are none to check. It holds the loops weakly."""
        if loops == "all":
            refuse = lambda loop: loop is not None  # None is outside any loop
        elif len(refs := WeakSet(loops)) > 0:
            assert all(isinstance(loop, asyncio.AbstractEventLoop) for loop in refs)
            refuse = refs.__contains__  # None, outside a loop, is never in refs
        else:
            return None
        return partial(cls._invariant, refuse)


# State trackers

class StateTrackerError(AsyncAnywhereException, asyncio.InvalidStateError, concurrent.futures.InvalidStateError):
    def __init__(
        self,
        source: StateTracker,
        msg: str,
        *states: State,
    ):
        desc = self._state_names(*states)
        errprefix = f"{source.__class__.__qualname__}[state={source.state.name}]"
        super().__init__(f"{errprefix}: {msg.strip(" :")}: {desc}")

    @classmethod
    def _state_names(cls, *states: State) -> str:
        assert len(states) > 0
        if len(states) == 1:
            return states[0].name
        return f'one of {", ".join(sorted(s.name for s in states))}'


# Executors


class WorkerStopped(AsyncAnywhereException, Exception):
    def __init__(self, summary: str, cause: BaseException | None = None):
        super().__init__(summary)
        self.__cause__ = cause


class CleanStop(WorkerStopped): ...


class WorkerBroken(WorkerStopped, ABC):
    """Why a worker can't be used at all: rather than refusing calls, it raises this for them, and for shutdown()."""


class WorkerForked(WorkerBroken):
    """Why a worker can't be used: this is a process forked from its own (see executors._fork_safety)."""

    def __init__(self, summary: str = "", cause: BaseException | None = None):
        super().__init__(summary or f"forked into process {os.getpid()}", cause)


class WorkerInterrupted(AsyncAnywhereException, concurrent.futures.CancelledError):
    """What a call's waiters get when an interrupt cancels it (see EventLoopExecutor._job()): a subclass, as
    wrap_future() remakes concurrent.futures.CancelledError itself as asyncio's for those awaiting it, losing its
    __cause__."""

    @classmethod
    def _from_cancellation(cls, cancelled: BaseException, stopped: WorkerStopped) -> Self:
        interrupted = cls(*cancelled.args).with_traceback(cancelled.__traceback__)  # Ending where the job was cancelled
        interrupted.__cause__ = stopped
        return interrupted
