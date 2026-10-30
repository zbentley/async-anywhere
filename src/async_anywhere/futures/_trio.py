from typing import Awaitable, TYPE_CHECKING

import attr

import asyncio

if TYPE_CHECKING:
    import trio as _trio

    from async_anywhere.futures import AwaitableFuture


@attr.define(eq=False, repr=False)  # Identity equality and hashing, as for any future
class TrioAwaitableFuture[T](Awaitable[T]):
    _source: AwaitableFuture[T] = attr.field()
    _token: _trio.lowlevel.TrioToken = attr.field()

    def cancel(self) -> bool:
        return self._source.cancel()

    def __await__(self):
        import trio as _trio
        # A done-callback per await, not per future: one giving up drops its own, leaving others' (and later ones)
        event, token = _trio.Event(), self._token

        def complete(_):
            try:
                token.run_sync_soon(event.set)
            except _trio.RunFinishedError:
                pass

        self._source.add_done_callback(complete)
        try:
            yield from event.wait().__await__()
        except _trio.Cancelled:
            if not self._source.cancel():  # As cancelling an asyncio waiter does: unless it's done, or refuses
                self._source.remove_done_callback(complete)  # Refused while pending: so it lets go of this wait
            raise
        return self._source.result()
