import asyncio
import concurrent.futures
from typing import Awaitable

# from async_anywhere._util import LockProtected


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
    """A concurrent.futures.Future that can also be awaited, on any loop, as asyncio.wrap_future() would (so cancelling
    the awaiting task cancels it). Waiting on it unfinished with result() or exception() on `loop`, which would block
    forever if that's the loop that finishes it, raises RuntimeError."""

    def __init__(self, loop: asyncio.AbstractEventLoop):
        super().__init__()
        self._loop = loop
    #     self._async_future_cache = LockProtected(dict())
    #
    # def as_async_future(self, loop: asyncio.AbstractEventLoop | None = None, require_running_loop=True) -> asyncio.Future[T]:
    #     """Returns an asyncio.Future that is linkeds to this AwaitableFuture.
    #
    #     If require_active_loop is True, "loop" must be a non-closed, running event loop.
    #     """
    #
    #     if loop is None:
    #         loop = asyncio.get_running_loop() if require_running_loop else asyncio.get_event_loop()
    #     elif require_running_loop and loop.is_closed():
    #         raise RuntimeError('Event loop is closed')
    #     with self._async_future_cache as cache:
    #         if not (cached := cache.get(loop)):
    #             cached = cache[loop] = asyncio.wrap_future(self, loop=loop)
    #         return cached

    def __await__(self):
        if self.done():  # Needing no loop: in a process forked from inside a task, asyncio has none running
            if self.cancelled():
                raise asyncio.CancelledError  # Asyncio's, as wrap_future() would give: result() raises concurrent.futures'
            return self.result()
        return (yield from asyncio.wrap_future(self).__await__())

    def result(self, timeout: float | None = None) -> T:
        self._check_blocking()
        return super().result(timeout)

    def exception(self, timeout: float | None = None) -> BaseException | None:
        self._check_blocking()
        return super().exception(timeout)

    def _check_blocking(self):
        if not self.done() and asyncio._get_running_loop() is self._loop:
            raise RuntimeError(f"Waiting for {self!r} on its own loop would block forever: await it instead")

