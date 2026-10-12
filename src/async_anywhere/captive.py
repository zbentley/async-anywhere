import asyncio
import logging
from abc import ABC
from concurrent.futures import Future as ConcurrentFuture
from functools import wraps, cached_property
from inspect import iscoroutinefunction
from threading import Thread
from typing import Awaitable, Callable, ClassVar, Concatenate, Self, cast, final

import attr

from async_anywhere._util import AwaitableFuture, run_on_loop, race


@attr.define(eq=False)
class BackgroundThreadWorker[V](ABC):
    _runner: asyncio.AbstractEventLoop = attr.field(init=False, default=None)
    _value: AwaitableFuture[V] = attr.field(init=False, factory=AwaitableFuture)
    _stopped: AwaitableFuture = attr.field(init=False, factory=AwaitableFuture)
    _thread: Thread = attr.field(init=False, default=None)

    _idle_timeout: ClassVar[float | None] = None # Override to change expiration time

    def __attrs_post_init__(self):
        _ = self._expire_tracker  # Prime cache
        runner = ConcurrentFuture()
        self._stopped.add_done_callback(self._on_stop)
        self._thread = Thread(target=self._run_captive, daemon=True, name=self.logger.name, args=(runner,))
        self.logger.info("Starting background thread")
        self._thread.start()
        try:
            runner.result(2)  # If a background thread can't start within 2 seconds, crash.
        except TimeoutError:
            ex = TimeoutError("Worker did not start event loop in time")
            runner.add_done_callback(lambda _: self._stopped.set_exception(ex))
            raise ex

    @cached_property
    def logger(self):
        return logging.getLogger(f"maffo.worker[{type(self).__name__}]")

    @cached_property
    def _expire_tracker(self):
        return _ExpireTracker(timeout=self._idle_timeout, stopped=self._stopped)

    @final
    def _on_stop(self, _):
        err = self._stopped.exception()
        if isinstance(err, _CleanStop):
            self.logger.info(err.args[0])
        else:
            self.logger.error("Stopping due to error:", exc_info=err)
        for task in asyncio.all_tasks(self._runner):
            task.cancel()  # Cancel in-flight jobs and the main runloop

    async def _run(self) -> None:
        """Overriders must:
        1. Call "_report_ready" with a captive value that will be supplied to bridge methods at an appropriate time.
        2. Only return once this client can be considered stopped.
        3. Exit promptly when cancelled (cleanup is fine, but don't swallow the CancelledError and keep running).

        The default implementation, which should *not* be called via super() in most implementations, just reports
        ready and waits until stopped.
        """
        self._report_ready(None)
        await self._stopped  # Simple "wait forever" shortcut; asyncio.Event would also work.

    @final
    def _report_ready(self, val: V) -> None:
        # NOTE: can only be called on the worker's captive thread/event loop context.
        if not self._value.done():
            self._expire_tracker.reschedule()
            self._value.set_result(val)

    @final
    def _run_captive(self, ready: ConcurrentFuture) -> None:

        async def _runloop():
            for fut in (self._stopped, self._value):
                if not fut.set_running_or_notify_cancel():
                    raise RuntimeError("Failed to start runloop future")
            self._runner = asyncio.get_running_loop()
            ready.set_result(None)
            try:
                await self._run()
            except asyncio.CancelledError:
                if not self._stopped.done():  # Not ours: expire() always resolves _stopped before cancelling
                    raise

        def _stop(val: str, exc: BaseException | None = None):
            if not self._stopped.done():  # expiration got there first
                if exc is None:
                    self._stopped.set_exception(_CleanStop(val))
                else:
                    # Other BaseExceptions (e.g. SystemExit) deliberately reach callers unwrapped.
                    if isinstance(exc, asyncio.CancelledError):  # Never hand callers a CancelledError
                        wrapped = RuntimeError(f"{val}: {exc!r}")
                        wrapped.__cause__ = exc
                        exc = wrapped
                    exc.add_note(val)
                    self._stopped.set_exception(exc)

        try:
            asyncio.run(_runloop())
            _stop("_run() returned")
        except BaseException as ex:
            _stop("unexpected error", ex)
            # NB: we intentionally don't re-raise here, since that clutters deliberately-induced crashes in pytest with
            # "background thread crashed" PytestUnhandledThreadExceptionWarning errors. Since the thread's about to shut
            # down anyway, this is harmless.

    @final
    def unsafe_shut_down(self, timeout_seconds: float=2):
        """Synchronously flags the worker to stop, and waits on it shutting down.

        In progress work will be signalled to abort via asyncio.CancelledError.

        NOTE: This method is racy, can corrupt data, and should only be called from tests!
        """
        if not self._stopped.done():
            run_on_loop(self._runner, None, self._expire_tracker.expire, "manually stopped")

        self._thread.join(timeout_seconds)
        if self._thread.is_alive():
            raise TimeoutError("Thread didn't stop in time")
        return self._stopped.exception()

    @final
    async def _bridge[_T, **_P](
        self, func: Callable[Concatenate[V, _P], Awaitable[_T]], *args: _P.args, **kwargs: _P.kwargs
    ) -> _T:
        # One of these two always resolves, and if it's _stopped it'll always raise an error:
        (value,) = await race(self._stopped, self._value)

        try:
            # Pause the timer and prepare to start the job
            wrapper = self._expire_tracker.call(self._runner)
            # Instantiate, but do not await, a coroutine that will run the function on the worker's captive loop.
            # This is done separately from running it so that we can handle the race condition wherein the worker's loop
            # crashed after we checked _stopped above.
            job = run_on_loop(self._runner, None, wrapper, func, value, *args, **kwargs)
        except RuntimeError:
            # "Raise" does nothing; the await itself raises, but otherwise the typechecker is mad:
            raise await self._stopped
        # _stopped only ever resolves with an exception, so race() either returns the job's result or raises. A stop
        # mid-call normally surfaces as the stop reason: _stopped resolves before _on_stop's cancellation of the job
        # can reach us. If the job's cancellation arrives first anyway (e.g. _run() returned and the loop was torn down
        # before _run_captive() recorded why), then unless our own caller is being cancelled, wait for and raise the
        # recorded reason; _run_captive() always resolves _stopped, so this can't dangle. The job wrapper turns a
        # bridged function's own CancelledError into a RuntimeError, so it can't be mistaken for either case.
        try:
            result, = await race(self._stopped, job)
            return result
        except asyncio.CancelledError:
            if (task := asyncio.current_task()) and task.cancelling():
                raise
            raise await self._stopped


def register_bridge_method[V, T, **P](
    func: Callable[Concatenate[V, P], Awaitable[T]],
) -> Callable[Concatenate[BackgroundThreadWorker[V], P], Awaitable[T]]:
    """Expose a worker coroutine method--written to run in the worker's own thread and event loop--to callers in any
    other thread or loop, which await it like a normal method of the worker object."""
    assert iscoroutinefunction(func)

    @wraps(func)
    async def inner(self: BackgroundThreadWorker[V], *args: P.args, **kwargs: P.kwargs) -> T:
        return await self._bridge(func, *args, **kwargs)

    return inner

class _CleanStop(RuntimeError):
    ...

@attr.define
class _ExpireTracker:
    """
    Helper class tightly coupled to BackgroundThreadWorker. It handles:
    1. Tracking expiration timers.
    2. The act of expiring a BackgroundThreadWorker (which is one-way; an expired worker cannot be resurrected and should be disposed of).
    3. Pausing expiration and resetting expiration timers around calls to bridged methods.
    """
    timeout: float | None = attr.field(validator=attr.validators.optional(attr.validators.ge(0)))
    stopped: AwaitableFuture = attr.field()
    calls: int = attr.field(default=0, init=False)
    _handle: asyncio.TimerHandle | None = attr.field(default=None, init=False)

    def expire(self, reason: str) -> None:
        """If not already stopped, shuts down the captive worker and cancels all tasks.
        NOTE: can only be called on the worker's captive thread/event loop context.
        """
        # We could technically just cancel the task and report the expiration later, but then there's a window while
        # the task is shutting down where we'll provide known-broken workers from the cache.
        if not self.stopped.done():
            if self.calls > 0:
                self.stopped.set_exception(RuntimeError(f"(interrupted) {reason}"))
            else:
                self.stopped.set_exception(_CleanStop(f"(idle) {reason}"))

    def cancel(self, increment_calls=False):
        # NOTE: can only be called on the worker's captive thread/event loop context.
        handle = self._handle
        if handle is not None and not handle.cancelled():
            handle.cancel()
            self._handle = None
        if increment_calls:
            self.calls += 1

    def reschedule(self):
        """Reschedules/restarts the expiration timer from zero.
        NOTE: can only be called on the worker's captive thread/event loop context.
        """
        self.cancel()
        if self.timeout is not None:
            self._handle = run_on_loop(asyncio.get_running_loop(), self.timeout, self.expire, "expired")

    def call[T](self, loop: asyncio.AbstractEventLoop):
        """Registers that a call is now in progress with the worker, and returns a
        callable which, when run (and awaited, if it is asynchronous), executes the registered call, then unregisters its
        in-flight status with the worker.

        While a call is in flight, expirations of the worker are suspended, producing the property that a worker crash
        during execution is never due to timeout. The expiration TTL is reset when calls complete. Multiple calls can be
        in flight (to bound concurrency, implement locking in wrapped methods).

        This method can be called from any thread/event loop. The produced callable should be dispatched to the
        appropriate event loop context/executor by the caller; this method will not automatically run it, nor will this
        method choose where it runs."""

        run_on_loop(loop, None, self.cancel, increment_calls=True)

        async def inner(func, *args, **kwargs) -> T:
            """Wraps the supplied coroutine in the following behavior:
            1. Check the stopped future right before running it, to narrow race conditions.
            2. After it is done, reschedule the expiration timer if needed.

            NOTE: can only be called on the worker's captive thread/event loop context.
            """

            # If it already stopped/expired/etc, raise and fail. There's still an inevitable TOCTOU race condition here,
            # but running this check inside the call() loop narrows it as much as possible. The outcome of hitting that
            # race is an implementation-dependent failure when calling a bridge method on a captive worker that crashed.
            # This class can prevent races when it comes to expiration (a worker won't expire during a bridge call), but
            # it might fail for some external reason, which is unavoidable--that's just the halting problem.
            if self.stopped.done():
                raise self.stopped.exception()

            try:
                return await func(*args, **kwargs)
            except asyncio.CancelledError as ex:
                if not asyncio.current_task().cancelling():
                    raise RuntimeError("Bridged function raised CancelledError") from ex
                raise
            finally:
                self.calls -= 1
                if self.calls == 0:
                    self.reschedule()
        return inner