"""This is the docstring for async_anywhere."""

__version__ = "0.0.0"

import asyncio
import asyncio.futures
import concurrent.futures
import concurrent.futures._base
from functools import partial
from typing import Awaitable, Literal, overload


@overload
async def gather(*, return_exceptions: bool = False) -> tuple[()]: ...
@overload
async def gather[T](*coros: Awaitable[T], return_exceptions: Literal[False] = False) -> tuple[T, ...]: ...
@overload
async def gather[T](*coros: Awaitable[T], return_exceptions: bool) -> tuple[T | BaseException, ...]: ...
async def gather[T](*coros: Awaitable[T], return_exceptions: bool = False) -> tuple[T | BaseException, ...]:
    """
    Convenience wrapper for asyncio.gather which behaves exactly as it does. Exists for two reasons:

    1. asyncio.gather without any awaitables is somewhat costly to call just to get an empty tuple back. This method
       is cheaper to call.
    2. asyncio.gather isn't a true async method; it's a synchronous method that returns a Future. That's usually
       desirable, but occasionally it makes some wrapping tools (e.g. asgiref.async_to_sync) not work, and also
       can confuse some type checkers. This is a regular async method, which helps with those situations.
    """
    if len(coros) == 0:
        return ()
    return tuple(await asyncio.gather(*coros, return_exceptions=return_exceptions))


# What chain_futures() does to one future as the other finishes: one of the first four, with NOTIFY or'd in, optionally
DO_NOTHING = 0
CANCEL_OTHER = 1
FAIL_OTHER = 2
COMPLETE_OTHER = 4
NOTIFY = 8


def _settle(future, action, value):
    # On its loop, for an asyncio future
    try:
        if action & CANCEL_OTHER:
            future.cancel()
        if action & NOTIFY and isinstance(future, concurrent.futures.Future):
            with future._condition:  # As an executor would: starting it, if it's to be finished, or announcing its cancellation
                state = future._state
                if state == concurrent.futures._base.CANCELLED or (
                    state == concurrent.futures._base.PENDING and action & (FAIL_OTHER | COMPLETE_OTHER)
                ):
                    future.set_running_or_notify_cancel()
        if action & FAIL_OTHER:
            future.set_exception(value)
        elif action & COMPLETE_OTHER:
            future.set_result(value)
    except (asyncio.InvalidStateError, concurrent.futures.InvalidStateError):
        pass  # Finished, or cancelled, already


def _chain_callback(other, on_cancel, on_fail, on_complete, done):
    # What's done to other, and with what: done's result or exception, or its cancellation, as result() raises it
    if done.cancelled():
        action = on_cancel
        try:
            done.result()
        except BaseException as ex:
            value = ex
    elif (value := done.exception()) is not None:
        action = on_fail
    else:
        action, value = on_complete, done.result()
    if action == DO_NOTHING:
        return
    into_asyncio = isinstance(other, asyncio.Future) or asyncio.isfuture(other)
    if action & FAIL_OTHER and into_asyncio != (isinstance(done, asyncio.Future) or asyncio.isfuture(done)):
        # Into the other kind of future, a cancellation becomes that kind's: as asyncio's own chaining does, and back
        if into_asyncio:
            value = asyncio.futures._convert_future_exc(value)
        elif type(value) is asyncio.CancelledError:
            value = concurrent.futures.CancelledError(*value.args).with_traceback(value.__traceback__)
    if into_asyncio and asyncio._get_running_loop() is not (loop := other.get_loop()):
        loop.call_soon_threadsafe(_settle, other, action, value)
        return
    _settle(other, action, value)


def chain_futures(
    source: concurrent.futures.Future | asyncio.Future,
    dest: concurrent.futures.Future | asyncio.Future,
    on_source_cancel: int = CANCEL_OTHER | NOTIFY,
    on_source_fail: int = FAIL_OTHER | NOTIFY,
    on_source_complete: int = COMPLETE_OTHER | NOTIFY,
    on_dest_cancel: int = CANCEL_OTHER,
    on_dest_fail: int = DO_NOTHING,
    on_dest_complete: int = DO_NOTHING,
):
    """
    Chain two futures, each of either kind (asyncio's, or concurrent.futures', AwaitableFutures included), so that each
    is acted on as the other finishes: dest by the on_source_* action for how source finished (cancelled, failed or
    completed), and source by the on_dest_* action for how dest did. By default, as asyncio's own chaining: dest
    finishes as source does, and cancelling dest cancels source.

    An action is DO_NOTHING, CANCEL_OTHER, or FAIL_OTHER or COMPLETE_OTHER, which fail or complete the other future with
    what the finished one had: its result, its exception, or, if it was cancelled, the CancelledError its result()
    raises. NOTIFY can be or'd into any of them (COMPLETE_OTHER | NOTIFY, say): then a concurrent.futures.Future acted
    on is treated as an executor treats its own, started before it's finished, and its cancellation, by anyone, made
    known to concurrent.futures.wait() and as_completed(), which otherwise never see it.

    - If source came from an executor, the on_dest_* actions must be DO_NOTHING or CANCEL_OTHER, without NOTIFY, as the
      executor finishes and notifies its futures itself, and breaks (losing a pool's thread, or its process pool) if
      that's been done already.
    - If dest came from an executor, the on_source_* actions must be DO_NOTHING or CANCEL_OTHER, without NOTIFY, for the
      same reason.
    - If source is waited on with concurrent.futures.wait() or as_completed(), and didn't come from an executor, every
      on_dest_* action must include NOTIFY.
    - If dest is waited on with concurrent.futures.wait() or as_completed(), and didn't come from an executor, every
      on_source_* action must include NOTIFY, as the defaults do.

    AwaitableFutures make their own cancellations known, so never need NOTIFY. Set on the other kind of future than the
    one it came from, a CancelledError becomes that kind's, as in asyncio.wrap_future(), and back. Actions run on the
    thread that finishes the future, or on the loop of an asyncio future acted on (raising RuntimeError, which the
    finished future logs, if that loop has closed). It can be called from any thread: off an asyncio future's loop, it
    registers with that future from the loop (raising RuntimeError if the loop has closed), as asyncio's
    add_done_callback() isn't thread-safe.
    """
    for action in (on_source_cancel, on_source_fail, on_source_complete, on_dest_cancel, on_dest_fail, on_dest_complete):
        if (action & (CANCEL_OTHER | FAIL_OTHER | COMPLETE_OTHER)).bit_count() > 1:
            raise ValueError(f"An action can only be one of CANCEL_OTHER, FAIL_OTHER and COMPLETE_OTHER, not {action}")
    for future, other, on_cancel, on_fail, on_complete in (
        (source, dest, on_source_cancel, on_source_fail, on_source_complete),
        (dest, source, on_dest_cancel, on_dest_fail, on_dest_complete),
    ):
        if on_cancel == on_fail == on_complete == DO_NOTHING:
            continue  # A direction that does nothing needs no callback
        callback = partial(_chain_callback, other, on_cancel, on_fail, on_complete)
        is_asyncio = isinstance(future, asyncio.Future) or asyncio.isfuture(future)
        if is_asyncio and asyncio._get_running_loop() is not (loop := future.get_loop()):
            loop.call_soon_threadsafe(future.add_done_callback, callback)  # As that isn't thread-safe
        else:
            future.add_done_callback(callback)
