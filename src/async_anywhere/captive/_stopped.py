import logging
from typing import TYPE_CHECKING, Collection, Literal

from async_anywhere._util import copy_exception
from async_anywhere.captive.exceptions import WorkerStopped, CleanStop, WorkerBroken
from async_anywhere.futures import AwaitableFuture

if TYPE_CHECKING:
    from async_anywhere.captive import CaptiveWorker


class WorkerStopFuture(AwaitableFuture[WorkerStopped]):
    """Why the worker stopped, once it has: a future whose result is the WorkerStopped it logs as it does, and which only
    fails, with a WorkerBroken, once the worker can't be used (as in a process forked from its own). It can be waited on
    from any thread, or awaited on any loop, but not cancelled. (CaptiveWorker._finished is one too, resolved with that
    WorkerStopped, unlogged, once the worker has finished.) Only the worker's own thread records stops (except in a
    process forked from the worker's: see _fork_safety), and only the first one counts. `calls` are those in
    flight, for the reasons it gives."""

    def __init__(self, worker: CaptiveWorker):
        # Uncancellable: waiters giving up (as wrap_future() does, once its awaiter is cancelled) mustn't undo it
        super().__init__(prevent_blocking_wait_in_loops=worker._loop, honor_cancel=False)
        self._log = worker._log
        self.interrupted = False  # Whether the worker has cancelled its parent task, calls, or what they left running

    def set_exception(self, exception):
        if not isinstance(exception, WorkerBroken):
            raise AssertionError(f"Invariant violation: {self!r} takes why the worker stopped as its result, not {exception!r}")
        super().set_exception(exception)

    def exception(self, timeout: float | None = None) -> WorkerBroken | None:
        return (error := super().exception(timeout)) and copy_exception(error)  # A copy, as for result()

    def result(self, timeout: float | None = None) -> WorkerStopped:
        # A copy, keeping the cause, each time: every raise adds its frames to the exception's traceback, so raising one
        # instance for every caller, on every thread, would pile theirs up on it. wrap_future() reads it through this too.
        if error := self.exception(timeout):
            raise error
        return copy_exception(super().result(timeout))

    def clean_stop(self, why: Literal["expired", "stopped", "parent task returned"], calls: Collection) -> bool:
        match why:
            case "expired":
                assert len(calls) == 0, "Invariant violation: expiry with calls in flight"
                reason = "(idle) expired"
            case "stopped":
                reason = "(waiting for running jobs) manually stopped" if len(calls) > 0 else "(idle) manually stopped"
            case "parent task returned":
                reason = f"({'interrupted' if len(calls) > 0 else 'idle'}) parent task returned"
        return self._stop(CleanStop(reason))

    def unclean_stop(self, reason: str, calls: Collection, cause: BaseException | None = None) -> bool:
        return self._stop(WorkerStopped(f"({'interrupted' if len(calls) > 0 else 'idle'}) {reason}", cause))

    def _stop(self, stop: WorkerStopped) -> bool:
        # Whether this was the first stop, so recorded
        if self.done():
            return False
        self.set_result(stop)
        if isinstance(stop, CleanStop):
            self._log(logging.INFO, stop.args[0])
        else:
            self._log(logging.ERROR, "Stopping due to error:", exc_info=stop)
        return True
