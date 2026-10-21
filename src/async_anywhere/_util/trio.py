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
            """AsyncioAwaitableFuture's counterpart for trio (anyio's trio backend included): one per trio run, whose waiters wait on a
            trio.Event that the source sets, from whichever thread finishes it, through the run's token. completed_at is the
            source's, read through, as only the source can finish it: so it's set once the source has finished, before the run
            has set the event (or if it never does, as the run finished first)."""
            _source: AwaitableFuture[T] = attr.field()
            _token: _trio.lowlevel.TrioToken = attr.field()
            _event: _trio.Event = attr.field(init=False, factory=_trio.Event)

            def __attrs_post_init__(self):
                def _complete(_):
                    try:
                        self._token.run_sync_soon(self._event.set)
                    except _trio.RunFinishedError:
                        pass

                self._source.add_done_callback(_complete)


            @property
            def completed_at(self) -> float | None:
                return self._source.completed_at

            def cancel(self) -> bool:
                return self._source.cancel()

            def __await__(self):
                try:
                    yield from self._event.wait().__await__()
                except _trio.Cancelled:
                    self._source.cancel()  # As cancelling an asyncio waiter does: unless it's done, or refuses
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

