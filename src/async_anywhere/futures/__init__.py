import asyncio
import concurrent.futures
from concurrent.futures._base import CANCELLED, CANCELLED_AND_NOTIFIED, FINISHED
from operator import attrgetter
from threading import Thread, current_thread
from time import monotonic
from typing import Awaitable

from async_anywhere._util import fast_runtime_checker, LoopSelector, ThreadSelector
from async_anywhere._util.trio import TrioRuntime
from async_anywhere.futures._asyncio import AsyncioAwaitableFuture

_template = concurrent.futures.Future()
_template.cancel()
_CANCELLED = frozenset((CANCELLED, CANCELLED_AND_NOTIFIED))
_DONE = _CANCELLED | {FINISHED}

class AwaitableFuture[T](concurrent.futures.Future[T], Awaitable[T]):
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
      waiters, alone. shielded() gives a future that can be cancelled without cancelling this one.
    - completed_at is the time.monotonic() time at which it finished (with a result, an exception, or cancellation), or
      None until then. Its trio futures, and shields that finish as it does (or that finish it, by cancelling it), share
      its time; shields that finish on their own (cancelled while it can't be, say) have their own. Its asyncio.Futures
      have none.

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

    __slots__ = (
        *sorted(key for key in vars(_template).keys() if key != '_state'),
        '_state_value',
        '_check_threads',
        '_check_loops',
        '_honor_cancel',
        '_source',  # What a shield finishes as (see shielded()), or None
        '_completed_at',
    )
    def __init__(
        self,
        *,
        prevent_blocking_wait_in_loops: LoopSelector = None,
        prevent_blocking_wait_in_threads: ThreadSelector = None,
        honor_cancel: bool = True,
    ):
        self._completed_at: float | None = None
        self._source: AwaitableFuture[T] | None = None
        super().__init__()
        # Not calling fast_runtime_checker() for None (most futures) saves its call: these are made on hot paths
        threads, loops = prevent_blocking_wait_in_threads, prevent_blocking_wait_in_loops
        self._check_threads = bool if threads is None else fast_runtime_checker(threads, Thread, current_thread)
        self._check_loops = bool if loops is None else fast_runtime_checker(
            loops, asyncio.AbstractEventLoop, asyncio._get_running_loop)
        self._honor_cancel = honor_cancel

    def _set_state(self, state: str):
        """Set the completed_at timestamp when the internal future's state is updated, under concurrent.futures.Future's
        lock. Setting it via a done callback would be racy (waiters in threads could observe the status change before
        completed_at was set) and skewed (on_completed would be set after state change), so we override some internals
        instead."""
        # Every completion sets this under self._condition, before notifying waiters or running callbacks: so it's set
        # once done() is True, for all of them
        if state == FINISHED or state == CANCELLED:
            # A shield finishing as its source has (see shielded()) shares its time
            self._completed_at = monotonic() if self._source is None else (self._source.completed_at or monotonic())
        self._state_value = state

    _state = property(attrgetter('_state_value'), _set_state)  # A C getter, as done() and the like read it often

    @property
    def completed_at(self) -> float | None:
        return self._completed_at

    def shielded(self) -> AwaitableFuture[T]:
        """
        Returns a new AwaitableFuture that finishes as this one does (with its result, exception, or cancellation), but
        whose cancellation leaves this one alone: as asyncio.shield() does for asyncio's awaitables. It prevents blocking
        waits in the same loops and threads as this one.

        Once the returned future is done (cancelled, say), this one drops its link to it, so a long-lived future doesn't
        keep every shield its waiters gave up on.
        """
        shield = AwaitableFuture()
        shield._source = self
        shield._check_loops = self._check_loops
        shield._check_threads = self._check_threads

        def finish(source: AwaitableFuture[T]):
            # Through the public methods, so that subclasses' (WorkerStopFuture's copies, say) are respected
            if source.cancelled():
                shield.cancel()
            elif (error := source.exception()) is not None:
                shield.set_exception_if_not_done(error)
            else:
                shield.set_result_if_not_done(source.result())

        def forget(_):
            # Under the lock, as concurrent.futures iterates _done_callbacks outside it once done, so mustn't lose one then
            with self._condition:
                if not self.done():
                    self._done_callbacks.remove(finish)

        self.add_done_callback(finish)
        shield.add_done_callback(forget)
        return shield

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
        is "refused" (the returned asyncio.Future's cancel() method returns False). If mutual cancellation behavior is
        not desired, use shielded().as_async_future(). If this one refuses cancellation while pending (made with
        honor_cancel=False, or running), only the returned future is cancelled, and this one lets go of it.

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
        if self.done():
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
        return self._state in _DONE

    def cancelled(self) -> bool:
        return self._state in _CANCELLED

    def cancel(self) -> bool:
        if not self._honor_cancel:
            return False
        cancelled = super().cancel()
        with self._condition:  # So only one of several threads cancelling it notifies
            if self._state == CANCELLED:
                # As an executor does on dequeuing it: until then, concurrent.futures.wait() and
                # concurrent.futures.as_completed() neither count it as done nor wake for it, and nothing here
                # dequeues it.
                self.set_running_or_notify_cancel()
        return cancelled

    def result(self, timeout: float | None = None) -> T:
        if self._state == FINISHED and self._exception is None:
            return self._result
        self._check_blocking()
        return super().result(timeout)

    def exception(self, timeout: float | None = None) -> BaseException | None:
        if self._state == FINISHED:
            return self._exception
        self._check_blocking()
        return super().exception(timeout)

    def raise_if_failed(self):
        """If this future was completed with an error, raise that error.
        If this future was cancelled, raise CancelledError."""
        if error := self.exception_or_none():
            raise error

    def set_result_if_not_done(self, result: T) -> bool:
        """Sets this future's completion state to "success", with the supplied value.
        Returns False if the future was already completed or cancelled.
        Returns True if the result was saved on the future.
        """
        if self.done():  # Lock-free, so cheaper than the raise below
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
        if self.done():
            return False
        try:
            self.set_exception(exception)
            return True
        except concurrent.futures.InvalidStateError:
            return False

    def result_or_none(self) -> T | None:
        """Returns the result of this future if it is done.
        Returns None if the future is still running. If the future failed or was cancelled, raises an exception.

        If your future was completed with the value None, this method is likely not useful.
        """
        return self.result() if self.done() else None

    def exception_or_none(self) -> BaseException | None:
        """Returns this future's exception if it failed with an error.
        Returns None if the future is still running, or if it completed without an exception.
        If the future was cancelled, a CancelledError will be raised.
        """
        return self.exception() if self.done() else None

    def _check_blocking(self):
        # This hokey control flow is intentional; it speeds up checks on happy paths.
        if (check_loops := self._check_loops) is (check_threads := self._check_threads) is bool or self.done():
            return
        if check_loops():
            raise RuntimeError(
                f"Waiting for {self!r} would deadlock: current event loop '{asyncio._get_running_loop()!r}' was set in 'prevent_blocking_wait_in_loops'")
        if check_threads():
            raise RuntimeError(
                f"Waiting for {self!r} would deadlock: current thread '{current_thread()!r}' was set in 'prevent_blocking_wait_in_threads'")