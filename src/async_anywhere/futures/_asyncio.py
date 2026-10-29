import asyncio
import asyncio.futures
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from async_anywhere.futures import AwaitableFuture

_get_running_loop = asyncio._get_running_loop
_convert_future_exc = asyncio.futures._convert_future_exc


class AsyncioAwaitableFuture[T](asyncio.Future[T]):
    """What AwaitableFuture.as_async_future() returns on asyncio: it finishes as its source does, at once if its source
    finishes on its loop's thread while that runs, or else in a callback queued on its loop, as asyncio.wrap_future()'s
    does. It holds its source until it's done, and its source holds it (through its done-callback) until the source is
    done."""

    __slots__ = ('_source',)

    def __init__(self, source: AwaitableFuture[T], loop: asyncio.AbstractEventLoop):
        super().__init__(loop=loop)
        self._source = source
        # Its loop's copy in a forked process never runs, so it's not run_in_forked_child
        source.add_done_callback(self._deliver, raise_if_failed_immediately=True)

    def cancel(self, msg=None) -> bool:
        """Changes cancellation behavior of asyncio.Future in two ways:
            1. Cancels the underlying concurrent.futures.Future right away, rather than in a later loop iteration.
            2. Refuses cancellation (returns False) if the underlying concurrent.futures.Future is already done.
        If the underlying future refuses cancellation while pending (running, or made with ignore_cancel=True), this one
        alone is cancelled, and the underlying one lets go of it.
        """
        if self.done():
            return False
        source = self._source
        if not source.cancel():
            with source._condition:  # Reentrant, so remove_done_callback() takes it too
                if source.done():
                    return False
                # Equal to the one __init__() added: same self and function. Gone already in a forked process, whose
                # fork handler dropped it: see _fork_safety
                source.remove_done_callback(self._deliver)
        self._source = None
        return super().cancel(msg)

    def _deliver(self, source: AwaitableFuture[T]):
        # source's done-callback (see __init__())
        loop = self.get_loop()
        if _get_running_loop() is not loop:
            try:
                loop.call_soon_threadsafe(self._finish)
            except RuntimeError:  # Closed
                pass
        elif not source.cancelled():
            self._finish()
        else:  # Cancelled: maybe by cancel(), which finishes this one itself (with its msg)
            loop.call_soon(self._finish)

    def _finish(self):
        # On its loop, which may have finished it meanwhile. Not through its own cancel(), which is for its users
        if self.done():
            return
        source, self._source = self._source, None
        # Through result() and exception(), so subclasses' overrides are respected
        if source.cancelled():
            super().cancel()
        elif (error := source.exception()) is not None:
            self.set_exception(_convert_future_exc(error))
        else:
            self.set_result(source.result())
