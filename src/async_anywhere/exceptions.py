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
from abc import ABC
from copy import copy
from typing import TYPE_CHECKING, final, Self

if TYPE_CHECKING:
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
        return AsyncAnywhereException.copy_exception(self, *causes)

    @final
    @staticmethod
    def copy_exception[E: BaseException](error: E, *causes: BaseException) -> E:
        """A copy of `error` to raise in its place: the interpreter changes an exception's traceback and context as it's
        raised, as add_note() does its notes, so raising one that others may be raising too would mix theirs into it. The
        copy starts from the original's traceback, and is raised from `causes`, each from the next, if any are given, or
        else from what the original was. Only for exceptions that copy.copy() round-trips: builtins and ours, not users'
        in general.

        :meta private:
        """
        new = copy(error).with_traceback(error.__traceback__)
        new.__context__ = error.__context__
        if len(notes := getattr(error, "__notes__", ())) > 0:
            new.__notes__ = list(notes)
        if len(causes) > 0:
            # Copying all but the last, whose cause is kept
            new.__cause__ = AsyncAnywhereException.copy_exception(*causes) if len(causes) > 1 else causes[0]
        else:
            new.__cause__, new.__suppress_context__ = error.__cause__, error.__suppress_context__
        return new


# Futures

class WouldBlock(AsyncAnywhereException, RuntimeError):
    """Raised when synchronously waiting for an :py:class:`AwaitableFuture` with ``.result()`` or ``.exception()``
    would block (that is, when a future is not done and a nonzero timeout is passed to those methods).

    The behavior of when and where this exception will be raised can be controlled via :py:attr:`AwaitableFuture.prevent_blocking_wait_when` parameter.
    """
    def __init__(self, future, culprit) -> None:
        """:meta private:"""
        # A thread or loop, or else AnyEventLoop or BlockingNotAllowed, from _util: named as users find them
        if getattr(culprit, "__module__", None) == "async_anywhere._util":  # Instances of builtins have none
            culprit = f"AwaitableFuture.{culprit.__name__}"
        else:
            culprit = repr(culprit)
        super().__init__(f"Waiting for {future!r} would deadlock: refused by {culprit}")


# State trackers

class StateTrackerError(AsyncAnywhereException, asyncio.InvalidStateError, concurrent.futures.InvalidStateError):
    r"""Raised when invalid invalid configuration is requested for or invalid actions are performed on a
    :py:class:`StateTracker`."""
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
    """
    Indicates that an :py:class:`EventLoopExecutor` has :py:mod:`stopped <async_anywhere.executors._stopped>`, and why.

    Raised (and sometimes returned) from various EventLoopExecutor functions, and may also be raised from :term:`job`
    futures when jobs are aborted due to executor stop."""
    def __init__(self, summary: str, cause: BaseException | None = None):
        """:meta private:"""
        super().__init__(summary)
        self.__cause__ = cause


class CleanStop(WorkerStopped):
    r"""Indicates that am :py:class:`EventLoopExecutor` stopped without interrupting any :term:`job`\ s (the
    :term:`main task` is always interrupted regardless)."""

class WorkerBroken(WorkerStopped, ABC):
    """Indicates that an :py:class:`EventLoopExecutor` cannot perform the requested action(s) because it was stopped
    due to an internal error or unsupported state."""

class WorkerForked(WorkerBroken):
    """Indicates that an :py:class:`EventLoopExecutor` cannot perform the requested action(s) because it was created in
    a parent process and cannot be used in a forked child. See
    :py:mod:`executor fork safety <async_anywhere.executors._fork_safety>` for more information."""

    def __init__(self, summary: str = "", cause: BaseException | None = None):
        """:meta private:"""
        super().__init__(summary or f"forked into process {os.getpid()}", cause)


class WorkerInterrupted(AsyncAnywhereException, concurrent.futures.CancelledError):
    r"""Raised from attempts to wait on an :py:class:`EventLoopExecutor`\ s :term:`job` that has been cancelled due to
    a worker stop condition that interrupts running work (e.g. ``EventLoopExecutor.shutdown(cancel_futures=True)``)."""

    @classmethod
    def from_cancellation(cls, cancelled: BaseException, stopped: WorkerStopped) -> Self:
        """:meta private:"""
        interrupted = cls(*cancelled.args).with_traceback(cancelled.__traceback__)  # Ending where the job was cancelled
        interrupted.__cause__ = stopped
        return interrupted
