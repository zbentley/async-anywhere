import asyncio
import concurrent.futures
from concurrent.futures._base import CANCELLED_AND_NOTIFIED, FINISHED, LOGGER, PENDING, RUNNING
from math import isinf
from threading import Condition
from time import monotonic
from typing import Any, Awaitable, Callable, Self

from async_anywhere._util import blocking_wait_checker, LoopSelector, ThreadSelector
from async_anywhere._util.trio import TrioRuntime
from async_anywhere.futures._asyncio import AsyncioAwaitableFuture
from async_anywhere.state_tracker import State, StateTracker, StateTrackerError


class AwaitableFuture[T](concurrent.futures.Future[T], StateTracker, Awaitable[T]):
    """A concurrent.futures.Future subclass that can also be awaited. Provides the following behaviors:

    - Calling "await AwaitableFuture(...)" can be done from any event loop, or trio task (anyio's included).
    - If an AwaitableFuture has already completed, "await"ing it is significantly cheaper than "await"ing an
      asyncio.Future or using asyncio.wrap_future.
    - Users can specify one or more event loops or threads within which blocking wait methods inherited from
      concurrent.futures.Future (e.g. fut.result(timeout=10)) will fail immediately if the invoking AwaitableFuture is
      not done. This turns a common cause of deadlocks (erroneously synchronously waiting for an incomplete future on
      the loop or thread which was intended to complete it) into an error.
    - Several other convenience methods to save on common future-related code.
    - Cancelling awaits on this class or asyncio.Futures provided by it immediately cancels the underlying
      concurrent.futures.Future, rather than postponing cancellation until a later event loop iteration as CPython does.
    - Finishing it on a running event loop's thread finishes the asyncio.Futures it provided on that loop at once, rather
      than in a later loop iteration as asyncio.wrap_future()'s are.
    - With honor_cancel=False, cancel() is refused (returns False), and waiters giving up leave it, and its other
      waiters, alone.
    - completed_at is the time.monotonic() time at which it finished (with a result, an exception, or cancellation), or
      None until then. Its trio futures share its time. Its asyncio.Futures have none.

    With honor_cancel=False, waiters giving up (as asyncio.wait_for(), asyncio.timeout(), TaskGroups, and trio's and
    anyio's cancel scopes make them) still do so at once, leaving it pending, as asyncio.shield() would. But cancel() is
    refused for everyone, its owner included: it returns False while neither running() nor done(), which a plain
    concurrent.futures.Future's never does, and most code ignores what cancel() returns. So:

    - Cancelling it, then waiting for it (with concurrent.futures.wait(), asyncio.wait(), or asyncio.gather(), as
      teardown code does for tasks) waits until it's finished some other way, if ever. To abort it, set an exception
      instead: a concurrent.futures.CancelledError reaches asyncio waiters already waiting as asyncio.CancelledError,
      but is raised as is to those that await it once it's done.
    - Some ways of waiting with a timeout leave a registration on it per wait, until it finishes: asyncio.wait() leaves
      each as_async_future() it's given (each logging "exception was never retrieved" if this one fails), and
      asyncio.wait_for(asyncio.wrap_future(...)) a callback, as it does on a running executor job's future.
      Awaiting it, or as_async_future(), under asyncio.wait_for() or asyncio.timeout() leaves nothing behind.
    """

    # Running is as an executor's job is once started, which can't be cancelled. Once done, it never transitions again.
    Pending = State(initial=True)
    Running = State(sources=Pending)
    Cancelled = State(sources=Pending)
    Succeeded = State(sources=(Pending, Running))  # Its transition's data is the result
    Failed = State(sources=(Pending, Running))  # Its transition's data is the exception
    # See _state. Cancelling notifies concurrent.futures' waiters at once, so it's never just CANCELLED.
    _CF_STATES = {
        Pending: PENDING, Running: RUNNING, Cancelled: CANCELLED_AND_NOTIFIED, Succeeded: FINISHED, Failed: FINISHED
    }

    __slots__ = (
        '_runtime_checker',  # Refuses blocking waits where they'd deadlock (see blocking_wait_checker()), or None
        '_honor_cancel',
        '_completed_at',
        # Its done callbacks, dropped once they've run, and concurrent.futures.wait()'s and as_completed()'s waiters:
        # both changed only under _condition
        '_done_callbacks',
        '_waiters',
    )
    def __init__(
        self,
        *,
        prevent_blocking_wait_in_loops: LoopSelector = None,
        prevent_blocking_wait_in_threads: ThreadSelector = None,
        honor_cancel: bool = True,
    ):
        # NB: we're deliberately not calling concurrent.future.Future's super().__init__. We replace basically all of
        # that class's internals, and subclass it only to satisfy isinstance() checks performed by stdlib. This class's
        # observable behavior contract (which, per the stdlib's documented warning, is NOT limited to public methods)
        # must remain a superset of concurrent.future.Future's contract.
        StateTracker.__init__(self, lock=Condition())
        # Not calling blocking_wait_checker() for None (most futures) saves its call: these are made on hot paths
        loops, threads = prevent_blocking_wait_in_loops, prevent_blocking_wait_in_threads
        self._runtime_checker = None if loops is threads is None else blocking_wait_checker(loops, threads)
        self._honor_cancel = honor_cancel
        self._completed_at: float | None = None
        self._done_callbacks: list[Callable[[Self], Any]] = []

        # Mutated only by code in concurrent.futures.wait() and .as_completed(). Contains opaque objects installed and
        # removed by those methods.
        self._waiters: list = []

    def __repr__(self):
        # Unlike concurrent.futures.Future, we don't need to take the lock for repr, since reading the field is atomic.
        # Our repr is similar to, but not the same as, concurrent.futures.Future's repr.
        _, state, data = self._last_transition
        outcome = "" if data is None else f"[{type(data).__name__}]"
        return f"<{type(self).__qualname__} at {id(self):#x} {state.name}{outcome}>"

    @property
    def _state(self) -> str:
        # What concurrent.futures.wait() and as_completed() read, under _condition, on every future they're given
        return self._CF_STATES[self.state]

    def after_state_change(self, old_state: State, new_state: State, data: Any):
        # Under _condition, so concurrent.futures.wait() and as_completed(), which add their waiters under it, miss nothing
        if new_state is self.Running:
            return
        self._completed_at = monotonic()
        for waiter in self._waiters:
            if new_state is self.Succeeded:
                waiter.add_result(self)
            elif new_state is self.Failed:
                waiter.add_exception(self)
            else:
                waiter.add_cancelled(self)
        self._condition.notify_all()

    @property
    def completed_at(self) -> float | None:
        with self._condition:  # Set just after it finishes, under this: so it's set for anyone who has seen it done
            return self._completed_at

    def as_async_future(
        self, loop: asyncio.AbstractEventLoop | None = None, require_running_loop=True
    ) -> asyncio.Future[T] | Awaitable[T]:
        """
        Returns an asyncio.Future that is linked to this AwaitableFuture on the specified event loop. Called in a trio
        task with no loop given (anyio's on trio, or a trio guest run's on an asyncio loop, included), it returns its trio
        counterpart for that run instead: awaitable, and cancellable, likewise, but not an asyncio.Future.

        Note that the returned future may not be new/unique; use wrap_future on the result of this method if a unique
        future is required.

        Cancelling the returned future cancels this one, unless this one has already finished, in which case cancellation
        is "refused" (the returned asyncio.Future's cancel() method returns False). If this one refuses cancellation while
        pending (made with honor_cancel=False, or running), only the returned future is cancelled, and this one lets go of
        it.

        :param loop: the event loop to which the returned future should be bound, or the current event loop if None.
        :param require_running_loop: if True, a RuntimeError will be raised if the event loop to be used is not running or is closed.
        :return: an asyncio.Future linked to the invocant.
        """

        # Trio first, as a guest run's tasks run on an asyncio loop's thread, while it's running
        if loop is None and TrioRuntime.in_task():
            return TrioRuntime.Future(self, TrioRuntime.current_token())
        if loop is None:
            loop = asyncio.get_running_loop() if require_running_loop else asyncio.get_event_loop()
        elif require_running_loop and loop.is_closed():
            raise RuntimeError('Event loop is closed')
        return AsyncioAwaitableFuture(self, loop)

    def __await__(self):
        # If we're already done, __await__ can resolve without dealing with any loop machinery at all
        if self._last_transition[1].terminal:
            return self._await_finished()
        # Not a generator, so that what awaits it awaits the asyncio.Future's own iterator directly
        return self.as_async_future().__await__()

    def _await_finished(self):
        # In trio, waiters get result()'s concurrent.futures.CancelledError, as TrioRuntime.Future's do
        if self.cancelled() and not TrioRuntime.in_task():
            # Since result() raises concurrent.futures.CancelledError, convert to asyncio.CancelledError here
            raise asyncio.CancelledError
        return self.result()
        yield  # A generator, for __await__ to return, that finishes at once

    # Lock-free reads of its state: it only ever moves forward, and a finished one's outcome never changes

    def done(self) -> bool:
        return self._last_transition[1].terminal

    def cancelled(self) -> bool:
        return self._last_transition[1] is self.Cancelled

    def running(self) -> bool:
        return self.state is self.Running

    def cancel(self) -> bool:
        # Lock-free unless it's pending, as Executor.map() cancels every future once it has its result
        if self._honor_cancel and self._last_transition[1] is self.Pending:
            try:
                self._transition(self.Cancelled)
            except StateTrackerError:  # It started, finished, or was cancelled, meanwhile
                pass
            else:
                self._invoke_callbacks()
                return True
        return self.cancelled()

    def result(self, timeout: float | None = None) -> T:
        try:
            _, state, data = self._last_transition
            if state is self.Succeeded:
                return data
            if state is self.Failed:
                raise data
            if state is self.Cancelled:
                raise concurrent.futures.CancelledError
            if not self._wait(timeout):
                raise TimeoutError
            # Finished now. Not self.result(), which would run a subclass's override (WorkerStopFuture's copying, say)
            # twice.
            return AwaitableFuture.result(self)
        finally:
            del self, data  # Else what this raises would hold them through its traceback (in a cycle, if it's data)

    def exception(self, timeout: float | None = None) -> BaseException | None:
        try:
            _, state, data = self._last_transition
            if state is self.Succeeded:
                return None
            if state is self.Failed:
                return data
            if state is self.Cancelled:
                raise concurrent.futures.CancelledError
            if not self._wait(timeout):
                raise TimeoutError
            return AwaitableFuture.exception(self)  # As in result()
        finally:
            del self  # As in result()

    def _wait(self, timeout: float | None) -> bool:
        # For result() and exception(), which raise what they must themselves, so that only their frames, which don't
        # keep it, are in the traceback: whether it finished within `timeout`
        if self._runtime_checker is not None:
            self._runtime_checker(self)
        if timeout is not None and isinf(timeout) and timeout > 0:  # -inf times out at once, as any negative timeout does
            timeout = None
        with self._condition:
            return self._condition.wait_for(self.done, timeout)

    def add_done_callback(self, fn: Callable[[Self], Any]):
        with self._condition:
            if not self._last_transition[1].terminal:
                self._done_callbacks.append(fn)
                return
        try:
            fn(self)
        except Exception:
            LOGGER.exception("exception calling callback for %r", self)

    def _invoke_callbacks(self):
        super()._invoke_callbacks()
        # Once it's done, nothing adds or removes callbacks: so it lets go of them, and what they hold, at once
        self._done_callbacks.clear()

    # For executors

    def set_running_or_notify_cancel(self) -> bool:
        # cancel() has notified concurrent.futures' waiters already: so this just refuses a cancelled future, each time
        with self._condition:
            if self.cancelled():
                return False
            self._transition(self.Running)  # Refused once it's running or finished
        return True

    def set_result(self, result: T):
        self._transition(self.Succeeded, result)
        self._invoke_callbacks()

    def set_exception(self, exception: BaseException):
        self._transition(self.Failed, exception)
        self._invoke_callbacks()

    def raise_if_failed(self):
        """If this future was completed with an error, raise that error.
        If this future was cancelled, raise CancelledError."""
        if error := self.exception_or_none():
            raise error
        if self.state is self.Cancelled:
            raise concurrent.futures.CancelledError

    def set_result_if_not_done(self, result: T) -> bool:
        """Sets this future's completion state to "success", with the supplied value.
        Returns False if the future was already completed or cancelled.
        Returns True if the result was saved on the future.
        """
        if self._last_transition[1].terminal:  # Lock-free, so cheaper than the raise below
            return False
        try:
            self.set_result(result)
            return True
        except concurrent.futures.InvalidStateError:
            return False

    def set_exception_if_not_done(self, exception: BaseException) -> bool:
        """Sets this future's completion state to "failed", with the supplied error.
        Returns False if the future was already completed or cancelled.
        Returns True if the error was saved on the future.
        """
        if self._last_transition[1].terminal:
            return False
        try:
            self.set_exception(exception)
            return True
        except concurrent.futures.InvalidStateError:
            return False

    def result_or_none(self) -> T | None:
        """Returns the result of this future if it is done.
        Returns None if the future is still running, failed, or was cancelled.

        If your future was completed with the value None, this method is likely not useful (use .result(timeout=0) and
        handle the error instead).
        """
        _, state, data = self._last_transition
        return data if state is self.Succeeded else None

    def exception_or_none(self) -> BaseException | None:
        """Returns this future's exception if it failed with an error.
        Returns None if the future is still running, was cancelled, or completed without an exception.
        """
        _, state, data = self._last_transition
        return data if state is self.Failed else None
