import concurrent.futures
from functools import partial
from typing import Awaitable

import attr

try:
    import trio as _trio

    def _set(func, arg, _):
        try:
            # The run has finished, so nothing in it is waiting
            func(arg)
        except _trio.RunFinishedError:
            pass

    class TrioRuntime:
        enabled = True
        current_token = _trio.lowlevel.current_trio_token
        in_task = _trio.lowlevel.in_trio_task

        @attr.define(frozen=True, eq=False, repr=False)  # Identity equality and hashing, as for any future
        class Future[T](Awaitable[T]):
            """AsyncioAwaitableFuture's counterpart for trio (anyio's trio backend included): one per trio run, whose waiters wait on a
            trio.Event that the source sets, from whichever thread finishes it, through the run's token."""
            _source: concurrent.futures.Future[T] = attr.field()
            _token: _trio.lowlevel.TrioToken = attr.field()
            _event: _trio.Event = attr.field(init=False, factory=_trio.Event)

            def __attrs_post_init__(self):
                self._source.add_done_callback(partial(_set, self._token.run_sync_soon, self._event.set))

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
        in_task = False.__class__
        Future = _fail

