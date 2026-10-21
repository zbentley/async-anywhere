import asyncio
from time import monotonic
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from async_anywhere.futures import AwaitableFuture

class AsyncioAwaitableFuture[T](asyncio.Future[T]):
    __slots__ = ('_source', '_completed_at')

    def __init__(self, source: AwaitableFuture[T], loop: asyncio.AbstractEventLoop):
        super().__init__(loop=loop)
        self._source = source
        self._completed_at: float | None = None
        asyncio.futures._chain_future(source, self)  # As asyncio.wrap_future() does

    @property
    def completed_at(self) -> float | None:
        """Its source's, if it finished as its source did (or cancelled it), or else its own time.monotonic() time, as it
        finished on its own (cancelled while its source can't be, say); None until then.

        Set before the super() call that finishes it, as that runs Python code (the loop's call_soon(), and whatever
        else gets a turn then: signal handlers, finalizers, other threads) once done() is True, and can raise out of it
        (KeyboardInterrupt, say) having finished it. Or-set, so that one refused as it's already done leaves it alone,
        without the cost of checking done(): racing completions, which asyncio doesn't support off its loop's thread
        anyway, would only differ by moments."""
        return self._completed_at

    def cancel(self, msg=None) -> bool:
        """Changes cancellation behavior of asyncio.Future in two ways:
            1. Cancels the underlying concurrent.futures.Future right away, rather than in a later loop iteration.
            2. Refuses cancellation (returns False) if the underlying concurrent.futures.Future is already done.
        """
        if self._source.cancel() or not self._source.done():
            self._completed_at = self._completed_at or self._source.completed_at or monotonic()
            return super().cancel(msg)
        return False

    def set_result(self, result: T):
        self._completed_at = self._completed_at or self._source.completed_at or monotonic()
        super().set_result(result)

    def set_exception(self, exception: BaseException):
        self._completed_at = self._completed_at or self._source.completed_at or monotonic()
        try:
            super().set_exception(exception)
        except BaseException:
            # Unless it raised having finished: it can refuse what it's given (TypeError) while still pending
            if not self.done():
                self._completed_at = None
            raise
