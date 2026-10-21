import concurrent.futures
import os
from typing import Self


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
