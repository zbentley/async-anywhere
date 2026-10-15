import concurrent.futures
import os
from copy import copy
from typing import Self


def copied[E: BaseException](error: E, *causes: BaseException) -> E:
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
        new.__cause__ = copied(*causes) if causes[1:] else causes[0]  # Copying all but the last, whose cause is kept
    else:
        new.__cause__, new.__suppress_context__ = error.__cause__, error.__suppress_context__
    return new


class WorkerStopped(Exception):
    def __init__(self, summary: str, cause: BaseException | None = None):
        super().__init__(summary)
        self.__cause__ = cause


class CleanStop(WorkerStopped): ...


class WorkerBroken(WorkerStopped):
    """Why a worker can't be used at all: rather than refusing calls, it raises this for them, and for stop()."""


class WorkerForked(WorkerBroken):
    """Why a worker can't be used: this is a process forked from its own (see _fork_safety)."""

    def __init__(self, summary: str = "", cause: BaseException | None = None):
        super().__init__(summary or f"forked into process {os.getpid()}", cause)


class WorkerInterrupted(concurrent.futures.CancelledError):
    """What a call's waiters get when an interrupt cancels it (see CaptiveWorker._job()): a subclass, as wrap_future()
    remakes concurrent.futures.CancelledError itself as asyncio's for those awaiting it, losing its __cause__."""

    @classmethod
    def from_cancellation(cls, cancelled: BaseException, stopped: WorkerStopped) -> Self:
        interrupted = cls(*cancelled.args).with_traceback(cancelled.__traceback__)  # Ending where the job was cancelled
        interrupted.__cause__ = stopped
        return interrupted
