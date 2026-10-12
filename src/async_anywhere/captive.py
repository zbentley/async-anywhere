import logging
from asyncio import CancelledError, current_task, wrap_future
from collections.abc import Callable, Coroutine
from concurrent.futures import Future
from contextlib import suppress
from functools import wraps
from threading import current_thread, Event, Thread
from typing import Any, ClassVar, Concatenate, Awaitable, Self

import anyio
from anyio.from_thread import BlockingPortal


"""A note on AnyIO

anyio is used here because it saves us having to write some frankly tricky and voluminous code. It brings the following
benefits:
- A captive runloop with hardened exit/crash handling.
- Hardened thread-safe ways to pass calls to that loop and get results back.
- A reschedulable timer (CancelScope).
- Intelligible stop semantics re: determining when tasks are done.

But it has the following costs:
- Code in bridge methods and _run is forcibly opted into the anyio cancellation model.
- By extension, non-anyio-intended async code may be running in those methods (e.g. in a library), which can cause problems.
- When shutdown has been called with interrupt=False, existing bridge calls cannot be cancelled fully. If thread A starts
  a bridge job, and thread B calls shutdown(interrupt=False), then thread A cancels its bridge call, that cancellation
  doesn't propagate into the worker. This is because anyio starts ignoring cancellations at portal stop.
- If the worker loop is *blocked* by synchronous code (in a bridge call or _run), the act of *creating* a bridge call
  will block on it, even if it is never waited on. This is because anyio's portal.start_task_soon implementation waits on the
  remote loop to accept the job. 

The last two are arguably anyio design defects.



"""


class WorkerStopped(Exception):
    """The worker has stopped, so a call was refused or cut short. `__cause__` is the error that stopped it, if any."""

    def __init__(self, summary: str, cause: BaseException | None = None):
        super().__init__(summary)
        self.__cause__ = cause


class CleanStop(WorkerStopped):
    """The worker stopped as intended: it idled out, `_run()` returned, or a shutdown that interrupted nothing."""


class Call[T](Awaitable[T]):
    """A `bridge`d call made from outside the worker's thread. Await it on any event loop, or block on `result()` or
    `exception()` from any thread but the worker's.

    Cancelling the awaiting task, or interrupting `result()`/`exception()` (their timeout included), cancels the call on
    the worker. That cancel makes a blocking round trip to the worker's loop, stalling the cancelling thread (or event
    loop) for as long as the worker's loop is blocked. Once an interrupting stop has begun it isn't forwarded: every call
    is being cancelled anyway."""

    def __init__(self, future: Future[T]):
        self.future = future

    def __await__(self):
        return wrap_future(self.future).__await__()

    def result(self, timeout: float | None = None) -> T:
        """The call's return value, waiting up to `timeout`. A timeout, or any other interruption, cancels the call."""
        return self._wait(self.future.result, timeout)

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """The call's exception, or `None` if it returned, waiting up to `timeout`. A timeout, or any other interruption,
        cancels the call."""
        return self._wait(self.future.exception, timeout)

    def _wait(self, wait, timeout):
        try:
            return wait(timeout)
        except BaseException:
            self.future.cancel()
            raise


def bridge[W: CaptiveWorker, **P, T](
        func: Callable[Concatenate[W, P], Coroutine[Any, Any, T]],
) -> Callable[Concatenate[W, P], Call[T] | Coroutine[Any, Any, T]]:
    """Run `func(worker, ...)` on the worker's loop, callable from any thread.

    From another thread, `worker.func(...)` returns a `Call`; making it blocks that thread until the worker's loop has
    started the call (a round trip, longer while that loop is blocked). On the worker's own thread it returns a coroutine
    to await, which runs in the awaiting task.

    The call raises `WorkerStopped` if the worker has stopped, or if an interrupting stop cuts it short; callers on the
    worker's own thread see that interruption as `CancelledError` instead, since it cancels their own task. If `func`
    raises `CancelledError`, or is cancelled by anything on the worker's loop besides its caller or a stop, the call
    raises `RuntimeError`. `func` is cancelled anyio-style: see `CaptiveWorker`."""

    @wraps(func)
    def call(worker: W, *args: P.args, **kwargs: P.kwargs) -> Call[T] | Coroutine[Any, Any, T]:
        return worker._call(worker._job, func, args, kwargs)

    return call


class CaptiveWorker:
    """A size-1 connection pool: runs `_run()` on its own thread and event loop until it returns, raises, idles out, or
    is shut down, and runs `bridge`d methods on that loop. Get instances with `get()`.

    Stopping cancels `_run()` and in-flight calls anyio-style, re-raising at every await until they exit: cleanup that
    must await needs `anyio.CancelScope(shield=True)`, and holds up shutdown (and any `get()` waiting to replace the
    worker) until it finishes. Blocking work on the worker's loop delays everything there, cancellation included."""

    idle_timeout: ClassVar[float] = float('inf')

    def __init__(self):
        self.logger = logging.getLogger(f"captive[{type(self).__name__}]")
        self._stopped: WorkerStopped | None = None  # Why the worker stopped, once it has
        self._calls = 0
        ready = Event()
        self._thread = Thread(target=self._main, args=(ready,), name=self.logger.name, daemon=True)
        self._thread.start()
        if not ready.wait(2):
            raise TimeoutError("Timed out waiting for worker start")

    @classmethod

    async def _run(self):
        """Overriders run their client here until cancelled; returning or raising stops the worker. See `CaptiveWorker`
        about cleanup. The default waits for the portal to stop."""
        await self._portal.sleep_until_stopped()

    def _reschedule(self, adjust_calls_by: int):
        """Restart the idle countdown with `_reschedule(0)`, e.g. from `_run()` on activity of its own. Non-zero values
        count calls into and out of flight, which suspend the countdown. Worker loop only."""
        self._calls += adjust_calls_by
        self._timer.deadline = float('inf') if self._calls else anyio.current_time() + self.idle_timeout

    async def shutdown(self, interrupt=False):
        """
        Idempotently stop the worker and wait for it to finish, raising an exception indicating why it was stopped.

        If interrupt=False, the worker will be prevented from accepting new jobs but existing jobs will be left alone
        to finish. If interrupt=True, all jobs will be canceled.

        With interrupt=True, can be called from any thread or event loop context.
        With interrupt=False, can only be called from contexts outside of the worker's loop (that is, it can't be called
        from a call stack that contains a bridge method invocation).

        From synchronous code, run it with e.g. `asyncio.run`. On the worker's own thread it can't wait for that thread
        to exit, so it raises once `_run()` has been told to stop. Starting it blocks the calling thread until the
        worker's loop is free (see `bridge`). Cancelling it (a timeout included) while it drains also stops the drain,
        leaving the worker refusing calls with `_run()` still running: call it again to finish the job.
        """
        call = self._call(self._shutdown, interrupt)
        with suppress(WorkerStopped):  # Refused by a stopped portal: an interrupting stop is already under way
            await call
        if isinstance(call, Call):  # Not on the worker's own thread, which can't wait for itself to exit
            while self._thread.is_alive():  # Ugly polling loop to wait for the daemon thread to exit
                await anyio.sleep(0.05)
        raise self._stopped

    async def _shutdown(self, interrupt: bool, _in_loop: bool):
        """On the worker's loop: record why, unless something already has, then end `_run()` and cancel calls in flight,
        or let those calls finish first. Repeatable, e.g. to resume a cancelled drain."""
        if self._calls and interrupt:
            reason = WorkerStopped("(interrupted) manually stopped")
        else:
            reason = CleanStop("(draining) manually stopped" if self._calls else "(idle) manually stopped")
        await self._stop(reason, interrupt=False)
        # Ugly polling loop to wait for calls to finish. We don't need to do better, as this method will be called
        # rarely, mostly in tests.
        while self._calls and not interrupt:
            await anyio.sleep(0.05)
        await self._stop(reason, interrupt=True)

    def _call[R](self, func: Callable[..., Coroutine[Any, Any, R]], *args: Any) -> Call[R] | Coroutine[Any, Any, R]:
        """Run `func(*args, in_loop)` on the worker's loop: on the worker's own thread (`in_loop`), as a coroutine to
        await; elsewhere, as a `Call`, which fails with why the worker stopped if its portal has."""
        if current_thread() is self._thread:
            return func(*args, True)
        try:
            future = self._portal.start_task_soon(func, *args, False)
        except RuntimeError:  # The portal has stopped, which only happens after _stop() recorded why
            future = Future()
            future.set_exception(self._stopped)
        return Call(future)

    def _main(self, ready: Event):
        try:
            anyio.run(self._host, ready)
        except BaseException as exc:
            # Should never happen, to my understanding.
            if not isinstance(exc, SystemExit):
                self.logger.error("Unexpected exception in anyio.run", exc_info=exc)
            raise

    async def _host(self, ready: Event):
        async with BlockingPortal() as self._portal:
            # The idle timer (deadline inf until _reschedule()), and _run()'s off switch: shielded from the portal's
            # cancellation, so only expiry, or stops after recording why, cancel it.
            self._timer = anyio.CancelScope(shield=True)
            self._reschedule(0)
            ready.set()  # ...go!
            del ready
            try:
                with self._timer:
                    await self._run()
            except CancelledError as ex:  # Not ours: _timer absorbs those
                await self._stop(WorkerStopped("_run() was cancelled externally", ex))
            except BaseException as ex:
                await self._stop(WorkerStopped(f"_run() raised {ex!r}", ex))
            else:
                await self._stop(CleanStop("_run() returned"))

    async def _stop(self, reason: WorkerStopped, interrupt=True):
        if not self._stopped:
            # Check if expiry got there first, and can't record why itself
            self._stopped = CleanStop("(idle) expired") if self._timer.cancel_called else reason
            if isinstance(reason, CleanStop):
                self.logger.info(self._stopped.args[0])
            else:
                self.logger.error("Stopping due to error:", exc_info=self._stopped)
        # Only interrupting stops stop the portal: a drain leaves it running, so callers can still cancel their calls,
        # and a retried shutdown can still interrupt them.
        if interrupt:
            self._timer.cancel()  # Kill the run loop. First: see _job()
            await self._portal.stop(cancel_remaining=True)

    async def _job(self, func, args, kwargs, in_loop: bool):
        if self._timer.cancel_called:  # Expired, though _host() may not have recorded it yet
            await self._stop(CleanStop("(idle) expired"))
        if self._stopped:
            raise self._stopped
        self._reschedule(1)
        try:
            return await func(self, *args, **kwargs)
        except CancelledError as ex:
            if in_loop and current_task().cancelling():
                raise  # This runs in our caller's own task, which is being cancelled (as a stop does, too)
            # Stops that cancel calls cancel _timer first; a drain sets _stopped but cancels nothing. So this is the
            # function's own doing, or a cancellation from our (other-loop) caller, whose future discards it anyway.
            if not self._timer.cancel_called:
                raise RuntimeError("Bridged function was cancelled, or raised CancelledError") from ex
        finally:
            self._reschedule(-1)
        raise self._stopped  # A stop cut this call short. Raised here, not in the except: it's shared
