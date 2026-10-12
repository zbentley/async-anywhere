"""Tests for AwaitableFuture on its own, without a worker."""
import asyncio
import threading

import pytest

from async_anywhere.futures import AwaitableFuture


@pytest.fixture
def owner():
    """The loop futures are bound to (for their blocking check), which never runs."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def test_awaiting_works_on_any_loop(owner):
    future = AwaitableFuture(owner)

    async def wait():
        return await future

    async def main():
        elsewhere = asyncio.ensure_future(asyncio.to_thread(asyncio.run, wait()))
        here = asyncio.ensure_future(wait())
        await asyncio.sleep(0.05)
        threading.Thread(target=future.set_result, args=("done",)).start()
        assert await here == "done" and await elsewhere == "done"

    asyncio.run(main())


def test_awaiting_a_done_future_needs_no_loop(owner):
    future = AwaitableFuture(owner)
    future.set_result("done")
    with pytest.raises(StopIteration) as stopped:
        future.__await__().send(None)  # Outside any loop
    assert stopped.value.value == "done"
    future = AwaitableFuture(owner)
    future.cancel()
    with pytest.raises(asyncio.CancelledError):  # asyncio's, as wrap_future() would give, not concurrent.futures'
        future.__await__().send(None)


@pytest.mark.parametrize("method", ["result", "exception"])
def test_waiting_synchronously_on_its_own_loop_is_an_error(owner, method):
    async def main():
        future = AwaitableFuture(asyncio.get_running_loop())
        with pytest.raises(RuntimeError, match="would block forever"):
            getattr(future, method)(0)
        with pytest.raises(TimeoutError):
            getattr(AwaitableFuture(owner), method)(0.01)  # Another loop's, so it waits as usual
        future.set_result("done")
        assert getattr(future, method)(0) == ("done" if method == "result" else None)  # Done, so it can't block

    asyncio.run(main())


def test_async_futures_are_one_per_loop(owner):
    future = AwaitableFuture(owner)

    async def main():
        here = future.as_async_future()
        assert future.as_async_future() is here and future.as_async_future(asyncio.get_running_loop()) is here

        async def elsewhere():
            there = future.as_async_future()
            return there is not here and there.get_loop() is asyncio.get_running_loop()

        assert await asyncio.to_thread(asyncio.run, elsewhere())
        future.set_result("done")
        assert await here == "done"

    asyncio.run(main())


def test_async_futures_follow_the_future(owner):
    async def main():
        done, failed, cancelled = futures = [AwaitableFuture(owner) for _ in range(3)]
        waiting = [future.as_async_future() for future in futures]
        for finish in lambda: done.set_result("done"), lambda: failed.set_exception(ValueError()), cancelled.cancel:
            threading.Thread(target=finish).start()  # From another thread, as a worker's would be
        results = await asyncio.gather(*waiting, return_exceptions=True)
        assert results[0] == "done" and type(results[1]) is ValueError and type(results[2]) is asyncio.CancelledError

    asyncio.run(main())


def test_cancelling_an_async_future_cancels_the_future(owner):
    async def main():
        future = AwaitableFuture(owner)
        future.as_async_future().cancel()
        await asyncio.sleep(0)  # For the cancellation to reach it
        assert future.cancelled()

    asyncio.run(main())


def test_async_futures_need_a_running_loop_unless_told_otherwise(owner):
    future = AwaitableFuture(owner)
    with pytest.raises(RuntimeError):
        future.as_async_future()  # None is running here
    closed = asyncio.new_event_loop()
    closed.close()
    with pytest.raises(RuntimeError, match="closed"):
        future.as_async_future(closed)
    current = asyncio.new_event_loop()
    asyncio.set_event_loop(current)
    try:
        assert future.as_async_future(require_running_loop=False).get_loop() is current
    finally:
        asyncio.set_event_loop(None)
        current.close()


@pytest.mark.xfail(strict=True, reason="BUG: require_running_loop refuses a closed loop, but not one that isn't running")
def test_async_futures_refuse_a_given_loop_that_isnt_running(owner):
    with pytest.raises(RuntimeError):
        AwaitableFuture(owner).as_async_future(owner)


@pytest.mark.xfail(strict=True, reason="BUG: a cached async future, once cancelled, is returned for good")
def test_an_async_future_cancelled_after_the_future_finished_isnt_returned_again(owner):
    async def main():
        future = AwaitableFuture(owner)
        future.set_result("done")
        future.as_async_future().cancel()  # As its awaiter is cancelled before the result reaches it
        await asyncio.sleep(0)
        assert future.result() == "done"
        assert await future.as_async_future() == "done"

    asyncio.run(main())


@pytest.mark.parametrize(
    "awaitable",
    [
        pytest.param(
            lambda future: future,
            id="awaited",
            marks=pytest.mark.xfail(strict=True, reason="BUG: awaiting goes through as_async_future()'s cache too"),
        ),
        pytest.param(
            lambda future: future.as_async_future(),
            id="as_async_future",
            marks=pytest.mark.xfail(
                strict=True, reason="BUG: one waiter giving up cancels the cached async future for all on its loop"
            ),
        ),
    ],
)
def test_giving_up_waiting_on_an_uncancellable_future_leaves_other_waiters_alone(owner, awaitable):
    async def main():
        future = AwaitableFuture(owner)
        assert future.set_running_or_notify_cancel()  # Running, as an executor's job would be, so it can't be cancelled

        async def wait():
            return await awaitable(future)

        other = asyncio.ensure_future(wait())
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(wait(), 0.05)
        future.set_result("done")
        assert await other == "done"
        assert await wait() == "done"

    asyncio.run(main())
