from typing import Awaitable, TYPE_CHECKING

import attr

if TYPE_CHECKING:
    from async_anywhere.futures import AwaitableFuture

try:
    import trio as _trio

    class TrioRuntime:
        enabled = True
        current_token = _trio.lowlevel.current_trio_token
        in_task = _trio.lowlevel.in_trio_task

        @attr.define(eq=False, repr=False)  # Identity equality and hashing, as for any future
        class Future[T](Awaitable[T]):
            """AsyncioAwaitableFuture's counterpart for trio (anyio's trio backend included): a new one per as_async_future()
            call in a trio task, each await on which waits on a
            trio.Event that the source sets, from whichever thread finishes it, through the run's token. completed_at is the
            source's, read through, as only the source can finish it: so it's set once the source has finished, before the run
            has set the event (or if it never does, as the run finished first)."""
            _source: AwaitableFuture[T] = attr.field()
            _token: _trio.lowlevel.TrioToken = attr.field()

            @property
            def completed_at(self) -> float | None:
                return self._source.completed_at

            def cancel(self) -> bool:
                return self._source.cancel()

            def __await__(self):
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
                        with self._source._condition:  # Refused while pending: so it lets go of this wait
                            if not self._source.done():
                                self._source._done_callbacks.remove(complete)
                    raise
                return self._source.result()

except ImportError:  # Then nothing can be awaiting in trio
    def _fail(*_, **__):
        raise RuntimeError("Trio is not installed")

    class TrioRuntime:
        enabled = False
        current_token = _fail
        in_task = bool
        Future = _fail

