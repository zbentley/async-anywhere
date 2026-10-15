import asyncio
import concurrent.futures
from concurrent.futures._base import CANCELLED
from typing import Awaitable, Iterable

from async_anywhere._util import LockProtected


class AwaitableFuture[T](concurrent.futures.Future[T], Awaitable[T]):
    """A concurrent.futures.Future subclass that can also be awaited. Provides the following behaviors:

    - Calling "await AwaitableFuture(...)" can be done from any event loop.
    - If an AwaitableFuture has already completed, "await"ing it is significantly cheaper than "await"ing an
      asyncio.Future.
    - Using the blocking wait methods inherited from concurrent.futures.Future will fail with a RuntimeError if an
      AwaitableFuture is not done, and the blocking wait is issued on a thread running one of the event loops given as
      prevent_blocking_wait_in_loops: those intended to eventually complete the AwaitableFuture from async code in that
      same loop. This helps prevent accidental deadlocks by surfacing them as errors instead of hangs.

    """

    def __init__(self, prevent_blocking_wait_in_loops: Iterable[asyncio.AbstractEventLoop] = ()):
        super().__init__()
        self._prevent_blocking_wait_in_loops = tuple(prevent_blocking_wait_in_loops)
        self._async_future_cache = LockProtected(dict())

    def as_async_future(self, loop: asyncio.AbstractEventLoop | None = None, require_running_loop=True) -> asyncio.Future[T]:
        """
        Returns an asyncio.Future that is linked to this AwaitableFuture on the specified event loop.

        Note that the returned future may not be new/unique; use wrap_future on the result of this method if a unique
        future is required.

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
                cached = cache[loop] = asyncio.wrap_future(self, loop=loop)
            return cached

    def __await__(self):
        # If we're already done, __await__ can resolve without dealing with any loop machinery at all
        if self.done():
            if self.cancelled():
                # Since result() raises concurrent.futures.CancelledError, convert to asyncio.CancelledError here
                raise asyncio.CancelledError
            return self.result()
        # Using "yield from" flags this function as a generator so that the above "return" works right:
        return (yield from asyncio.wrap_future(self).__await__())

    def cancel(self) -> bool:
        cancelled = super().cancel()
        with self._condition:  # So only one of several threads cancelling it notifies
            if self._state == CANCELLED:
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
        if error := self.exception_or_none():
            raise error

    def set_result_if_not_done(self, result: T) -> bool:
        try:
            self.set_result(result)
            return True
        except concurrent.futures.InvalidStateError:
            return False

    def set_exception_if_not_done(self, exception: BaseException) -> bool:
        try:
            self.set_exception(exception)
            return True
        except concurrent.futures.InvalidStateError:
            return False

    def result_or_none(self) -> T | None:
        return self.result() if self.done() else None

    def exception_or_none(self) -> BaseException | None:
        return self.exception() if self.done() else None  # What it failed with, once done: cancelled, it raises

    def _check_blocking(self):
        if not self.done() and (loop := asyncio._get_running_loop()) in self._prevent_blocking_wait_in_loops:
            raise RuntimeError(f"Waiting for {self!r} would deadlock: event loop '{loop!r}' was set in 'prevent_blocking_wait_in_loops'")

