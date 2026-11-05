r"""
Note:
    Users interacting with exception classes defined in this module must follow these rules:

    - Do not subclass or manually construct :py:class:`AsyncAnywhereException` or any subclass thereof from user code.
    - Treat instances of :py:class:`AsyncAnywhereException` as immutable. Changing any fields on them may break
      functionality. Standard automatic mutation via the Python runtime (e.g. call stack manipulation) is permitted.

"""
from __future__ import annotations

import asyncio
import concurrent.futures
import copyreg
import os
from abc import ABC, ABCMeta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Self

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
class AsyncAnywhereException(ABC, Exception):
    r"""
    All custom (non-stdlib) exceptions raised by this library are subclasses of this class.

    All :py:class:`AsyncAnywhereException`\ s can be pickled/unpickled/copied and shared between processes.

    Code in this library that raises exceptions (including stdlib Exceptions and :py:class:`AsyncAnywhereException`\ s)
    does so in a way that does not artificially prolong object lifetimes (e.g. this library will not raise errors whose
    handling or storage results in an event loop not being garbage collected when it should be). If you find that this is
    not the case in a specific situation, file a bug.
    """

    __slots__ = ()

    def __reduce__(self):
        # For copy and pickle: recreate from args (for some, a formatted message) without calling __init__, which may
        # not take them
        return copyreg.__newobj__, (type(self), *self.args), vars(self)

    def copy(self, *causes: BaseException) -> Self:
        """Returns a deep copy of this exception object, optionally with additional ``__causes__`` added to its lineage.

        :meta private:
        """
        from async_anywhere._util import copy_exception  # Only on the sad paths that copy: see the module's docstring

        return copy_exception(self, *causes)


# Futures

class WouldBlock(AsyncAnywhereException, RuntimeError):
    def __init__(self, future, culprit) -> None:
        """:meta private:"""
        super().__init__(f"Waiting for {future!r} would deadlock: refused by {culprit!r}")


# State trackers

class StateTrackerError(AsyncAnywhereException, asyncio.InvalidStateError, concurrent.futures.InvalidStateError):
    def __init__(
        self,
        source: StateTracker,
        msg: str,
        *states: State,
    ):
        """:meta private:"""
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
        """:meta private:"""
        super().__init__(summary)
        self.__cause__ = cause


class CleanStop(WorkerStopped): ...


class WorkerBroken(WorkerStopped, ABC):
    """Why a worker can't be used at all: rather than refusing calls, it raises this for them, and for shutdown()."""


class WorkerForked(WorkerBroken):
    """Why a worker can't be used: this is a process forked from its own (see executors._fork_safety)."""

    def __init__(self, summary: str = "", cause: BaseException | None = None):
        """:meta private:"""
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
