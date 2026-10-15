import asyncio
import concurrent.futures
from concurrent.futures._base import CANCELLED
from typing import Awaitable, Iterable

from async_anywhere._util import LockProtected


# def chain_futures(
#     source,
#     dest,
#     on_source_success=None,
#     on_source_exception=None,
#     on_source_cancelled=None,
#     on_dest_success=None,
#     on_dest_exception=None,
#     on_dest_cancelled=None,
# ):
#     """
#     Added callbacks are idempotent to the best of their ability, but it's still not recommended to call this function on
#     a source or destination that have already been wrapped with chain_futures or asyncio.wrap_future, as the resulting
#     behavior is confusing and likely not intended.
#
#     :param source:
#     :param dest:
#     :return:
#     """



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
        if self.done():  # Needing no loop: in a process forked from inside a task, asyncio has none running
            if self.cancelled():
                raise asyncio.CancelledError  # Asyncio's, as wrap_future() would give: result() raises concurrent.futures'
            return self.result()
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

    def failure(self) -> BaseException | None:
        return self.exception() if self.done() else None  # What it failed with, once done: cancelled, it raises

    def _check_blocking(self):
        if not self.done() and (loop := asyncio._get_running_loop()) in self._prevent_blocking_wait_in_loops:
            raise RuntimeError(f"Waiting for {self!r} on {loop!r}, which is to finish it, would block forever: await it")

