import asyncio
import concurrent.futures
from threading import Thread, current_thread
from typing import Awaitable, Iterable, Collection

from async_anywhere._util import LockProtected, fast_collection


class _AsyncFuture[T](asyncio.Future[T]):
    __slots__ = ('_source',)

    def __init__(self, source: concurrent.futures.Future[T], loop: asyncio.AbstractEventLoop):
        super().__init__(loop=loop)
        self._source = source
        asyncio.futures._chain_future(source, self)  # As asyncio.wrap_future() does

    def cancel(self, msg=None) -> bool:
        """Changes cancellation behavior of asyncio.Future in two ways:
            1. Cancels the underlying concurrent.futures.Future right away, rather than in a later loop iteration.
            2. Refuses cancellation (returns False) if the underlying concurrent.futures.Future is already done.
        """
        if self._source.cancel() or not self._source.done():
            return super().cancel(msg)
        return False

_template = concurrent.futures.Future()
_template.cancel()

class AwaitableFuture[T](concurrent.futures.Future[T], Awaitable[T]):
    """A concurrent.futures.Future subclass that can also be awaited. Provides the following behaviors:

    - Calling "await AwaitableFuture(...)" can be done from any event loop.
    - If an AwaitableFuture has already completed, "await"ing it is significantly cheaper than "await"ing an
      asyncio.Future or using asyncio.wrap_future.
    - Users can specify one or more event loops or threads within which blocking wait methods inherited from
      concurrent.futures.Future (e.g. fut.result(timeout=10)) will fail immediately if the invoking AwaitableFuture is
      not done. This turns a common cause of deadlocks (erroneously synchronously waiting for an incomplete future on
      the loop or thread which was intended to complete it) into an error.
    - Several other convenience methods to save on common future-related code.
    - Cancelling awaits on this class or asyncio.Futures provided by it immediately cancels the underlying
      concurrent.futures.Future, rather than postponing cancellation until a later event loop iteration as CPython does.
    """

    __slots__ = (
        *sorted(vars(_template).keys()),
        '_prevent_blocking_wait_in_threads',
        '_prevent_blocking_wait_in_loops',
        '_async_future_cache',
    )

    @staticmethod
    def _blocking_checker[K](vals: K | Iterable[K], kind: type[K]) -> Collection[K] | None:
        vals = fast_collection((vals,) if isinstance(vals, kind) else vals)
        assert all(isinstance(val, kind) for val in vals)
        return vals or None

    def __init__(
        self,
        prevent_blocking_wait_in_loops: asyncio.AbstractEventLoop | Iterable[asyncio.AbstractEventLoop] = (),
        prevent_blocking_wait_in_threads: Thread | Iterable[Thread] = (),
    ):
        super().__init__()

        self._prevent_blocking_wait_in_threads = self._blocking_checker(prevent_blocking_wait_in_threads, Thread)
        self._prevent_blocking_wait_in_loops = self._blocking_checker(prevent_blocking_wait_in_loops, asyncio.AbstractEventLoop)
        self._async_future_cache = LockProtected(dict())

    def as_async_future(self, loop: asyncio.AbstractEventLoop | None = None, require_running_loop=True) -> asyncio.Future[T]:
        """
        Returns an asyncio.Future that is linked to this AwaitableFuture on the specified event loop.

        Note that the returned future may not be new/unique; use wrap_future on the result of this method if a unique
        future is required.

        Cancelling the returned future cancels this one, unless this one has already finished, in which case cancellation
        is "refused" (the returned asyncio.Future's cancel() method returns False). If mutual cancellation behavior is
        not desired, use a different awaitable or wrap the result of this method in asyncio.shield.

        :param loop: the event loop to which the returned future should be bound, or the current event loop if None.
        :param require_running_loop: if True, a RuntimeError will be raised if the event loop to be used is not running or is closed.
        :return: an asyncio.Future linked to the invocant.
        """

        if loop is None:
            loop = asyncio.get_running_loop() if require_running_loop else asyncio.get_event_loop()
        elif require_running_loop and loop.is_closed():
            raise RuntimeError('Event loop is closed')
        with self._async_future_cache as cache:
            if not (cached := cache.get(loop)):
                cached = cache[loop] = _AsyncFuture(self, loop)
            return cached

    def __await__(self):
        # If we're already done, __await__ can resolve without dealing with any loop machinery at all
        if self.done():
            if self.cancelled():
                # Since result() raises concurrent.futures.CancelledError, convert to asyncio.CancelledError here
                raise asyncio.CancelledError
            return self.result()
        # Using "yield from" flags this function as a generator so that the above "return" works right:
        return (yield from self.as_async_future().__await__())

    def cancel(self) -> bool:
        cancelled = super().cancel()
        with self._condition:  # So only one of several threads cancelling it notifies
            if self._state == _template._state:
                # As an executor does on dequeuing it: until then, concurrent.futures.wait() and
                # concurrent.futures.as_completed() neither count it as done nor wake for it, and nothing here
                # dequeues it.
                self.set_running_or_notify_cancel()
        return cancelled

    def result(self, timeout: float | None = None) -> T:
        self._check_blocking()
        return super().result(timeout)

    def exception(self, timeout: float | None = None) -> BaseException | None:
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
        if (loops := self._prevent_blocking_wait_in_loops) is (threads := self._prevent_blocking_wait_in_threads) is None or self.done():
            return
        if (
            loops is not None
            and (loop := asyncio._get_running_loop()) is not None
            and loop in loops
        ):
            raise RuntimeError(
                f"Waiting for {self!r} would deadlock: current event loop '{loop!r}' was set in 'prevent_blocking_wait_in_loops'")
        if (
            threads is not None
            and current_thread() in threads
        ):
            raise RuntimeError(
                f"Waiting for {self!r} would deadlock: current thread '{current_thread()!r}' was set in 'prevent_blocking_wait_in_threads'")