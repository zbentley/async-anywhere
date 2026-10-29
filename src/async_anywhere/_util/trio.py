from importlib.util import find_spec
from typing import Awaitable, Callable, TYPE_CHECKING

import attr
import sniffio

if TYPE_CHECKING:
    import trio as _trio

    from async_anywhere.futures import AwaitableFuture


class TrioRuntime:
    current_token: Callable[[], _trio.lowlevel.TrioToken]  # trio's own, bound by _in_trio_task() on first use

    if find_spec("trio") is None:  # Then nothing can be awaiting in trio
        in_task = bool
    else:
        @staticmethod
        def in_task() -> bool:
            # trio.run() and guest ticks set it: cheaper than in_trio_task(), which raises internally outside trio
            return sniffio.thread_local.name == "trio" and TrioRuntime._in_trio_task()

    @staticmethod
    def _in_trio_task() -> bool:
        # Only reached in a trio run, which has imported trio: so unused or absent, trio costs nothing
        import trio as _trio
        TrioRuntime._in_trio_task = _trio.lowlevel.in_trio_task  # In place of this one, from now on
        TrioRuntime.current_token = staticmethod(_trio.lowlevel.current_trio_token)
        return _trio.lowlevel.in_trio_task()

    @attr.define(eq=False, repr=False)  # Identity equality and hashing, as for any future
    class Future[T](Awaitable[T]):
        """AsyncioAwaitableFuture's counterpart for trio (anyio's trio backend included): a new one per as_async_future()
        call in a trio task, each await on which waits on a
        trio.Event that the source sets, from whichever thread finishes it, through the run's token."""
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
