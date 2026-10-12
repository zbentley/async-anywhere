from asyncio import AbstractEventLoop
from logging import Logger
from typing import Never, Collection, Literal

from async_anywhere.captive._exceptions import WorkerStopped, CleanStop, copied
from async_anywhere.futures import AwaitableFuture


class WorkerStopFuture(AwaitableFuture[Never]):
    """Why the worker stopped, once it has: a future that only ever fails, with the WorkerStopped it logs as it does. It
    can be waited on from any thread, or awaited on any loop, but not cancelled. (CaptiveWorker._finished is one too,
    failed with that WorkerStopped, unlogged, once the worker has finished.) Only the worker's own thread records
    stops (except in a process forked from the worker's: see CaptiveWorker._after_fork()), and only the first one counts.
    `calls` are those in flight, for the reasons it gives."""

    def __init__(self, loop: AbstractEventLoop, logger: Logger):
        super().__init__(loop)
        self._logger = logger

    def cancel(self):
        return False  # Waiters giving up (as wrap_future() does, once its awaiter is cancelled) mustn't undo it

    def exception(self, timeout: float | None = None) -> WorkerStopped:
        # A copy, keeping the cause, each time: every raise adds its frames to the exception's traceback, so raising one
        # instance for every caller, on every thread, would pile theirs up on it. wrap_future() reads it through this too.
        return copied(super().exception(timeout))

    def result(self, timeout: float | None = None) -> Never:
        raise self.exception(timeout)

    def clean_stop(self, why: Literal["expired", "stopped", "_run() returned"], calls: Collection) -> bool:
        match why:
            case "expired":
                assert not calls, "Invariant violation: expiry with calls in flight"
                reason = "(idle) expired"
            case "stopped":
                reason = "(waiting for running jobs) manually stopped" if calls else "(idle) manually stopped"
            case "_run() returned":
                reason = f"({'interrupted' if calls else 'idle'}) _run() returned"
        return self._stop(CleanStop(reason))

    def unclean_stop(self, reason: str, calls: Collection, cause: BaseException | None = None) -> bool:
        return self._stop(WorkerStopped(f"({'interrupted' if calls else 'idle'}) {reason}", cause))

    def _stop(self, stop: WorkerStopped) -> bool:
        # Whether this was the first stop, so recorded
        if self.done():
            return False
        self.set_exception(stop)
        if isinstance(stop, CleanStop):
            self._logger.info(stop.args[0])
        else:
            self._logger.error("Stopping due to error:", exc_info=stop)
        return True
