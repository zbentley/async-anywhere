import asyncio
import asyncio.futures
import concurrent.futures
import concurrent.futures._base
import enum
from functools import partial

class chain(enum.IntFlag):  # noqa: N801 (Lower case, deliberately)
    DO_NOTHING = 0
    CANCEL_OTHER = 1
    FAIL_OTHER = 2
    COMPLETE_OTHER = 4
    NOTIFY = 8
    # Internally, actions are plain ints: chain's own bitwise operations cost ~40 times as much
    _EXCLUSIVE = enum.nonmember(CANCEL_OTHER | FAIL_OTHER | COMPLETE_OTHER)
    _FINISHING = enum.nonmember(FAIL_OTHER | COMPLETE_OTHER)

    @staticmethod
    def _settle(future, action, value):
        # On its loop, for an asyncio future
        try:
            if action & chain.CANCEL_OTHER._value_:
                future.cancel()
            if action & chain.NOTIFY._value_ and isinstance(future, concurrent.futures.Future):
                with future._condition:  # TODO doc why
                    state = future._state
                    if state == concurrent.futures._base.CANCELLED or (
                        state == concurrent.futures._base.PENDING and action & chain._FINISHING
                    ):
                        future.set_running_or_notify_cancel()
            if action & chain.FAIL_OTHER._value_:
                future.set_exception(value)
            elif action & chain.COMPLETE_OTHER._value_:
                future.set_result(value)
        except (asyncio.InvalidStateError, concurrent.futures.InvalidStateError):
            pass  # Finished, or cancelled, already

    @staticmethod
    def _chain_callback(other, on_cancel, on_fail, on_complete, done):
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
        if action == chain.DO_NOTHING._value_:
            return
        into_asyncio = isinstance(other, asyncio.Future) or asyncio.isfuture(other)
        if action & chain.FAIL_OTHER._value_ and into_asyncio != (isinstance(done, asyncio.Future) or asyncio.isfuture(done)):
            # Into the other kind of future, a cancellation becomes that kind's: as asyncio's own chaining does, and back
            if into_asyncio:
                value = asyncio.futures._convert_future_exc(value)
            elif type(value) is asyncio.CancelledError:
                value = concurrent.futures.CancelledError(*value.args).with_traceback(value.__traceback__)
        if into_asyncio and asyncio._get_running_loop() is not (loop := other.get_loop()):
            loop.call_soon_threadsafe(chain._settle, other, action, value)
            return
        chain._settle(other, action, value)

    @classmethod
    def link_futures(
        cls,
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
        actions = tuple(
            map(int, (on_source_cancel, on_source_fail, on_source_complete, on_dest_cancel, on_dest_fail, on_dest_complete))
        )
        for action in actions:
            if (action & cls._EXCLUSIVE).bit_count() > 1:
                raise ValueError(f"An action can only be one of CANCEL_OTHER, FAIL_OTHER and COMPLETE_OTHER, not {action}")
        for future, other, (on_cancel, on_fail, on_complete) in ((source, dest, actions[:3]), (dest, source, actions[3:])):
            if on_cancel == on_fail == on_complete == cls.DO_NOTHING._value_:
                continue  # A direction that does nothing needs no callback
            callback = partial(cls._chain_callback, other, on_cancel, on_fail, on_complete)
            is_asyncio = isinstance(future, asyncio.Future) or asyncio.isfuture(future)
            if is_asyncio and asyncio._get_running_loop() is not (loop := future.get_loop()):
                loop.call_soon_threadsafe(future.add_done_callback, callback)  # As that isn't thread-safe
            else:
                future.add_done_callback(callback)
