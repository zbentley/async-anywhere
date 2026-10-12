import asyncio
import concurrent.futures
from typing import Awaitable


class AwaitableFuture[T](concurrent.futures.Future[T], Awaitable[T]):
    """A concurrent.futures.Future that can also be awaited, on any loop, as asyncio.wrap_future() would (so cancelling
    the awaiting task cancels it). Waiting on it unfinished with result() or exception() on `loop`, which would block
    forever if that's the loop that finishes it, raises RuntimeError."""

    def __init__(self, loop: asyncio.AbstractEventLoop):
        super().__init__()
        self._loop = loop

    def as_async_future(self, loop: asyncio.AbstractEventLoop | None = None, require_open_loop=True) -> asyncio.Future[T]:
        """Returns an asyncio.Future that is linkeds to this AwaitableFuture.

        If require_active_loop is True, "loop" must be a non-closed, running event loop.
        """

        if loop is None:
            loop = asyncio.get_running_loop() if require_open_loop else asyncio.get_event_loop()
        elif require_active_loop: # TODO TODO TODO
            loop.is_closed() # TODO
        return asyncio.wrap_future(self, loop=loop)

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

