import asyncio
import concurrent.futures

class AsyncioAwaitableFuture[T](asyncio.Future[T]):
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
