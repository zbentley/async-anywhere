"""Tests for AwaitableFuture on its own, without a worker."""
import asyncio
import concurrent.futures
import threading

import pytest

from async_anywhere.futures import AwaitableFuture


@pytest.fixture
def owner():
    """A loop that never runs."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def test_awaiting_works_on_any_loop(owner):
    future = AwaitableFuture()

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
    future = AwaitableFuture()
    future.set_result("done")
    with pytest.raises(StopIteration) as stopped:
        future.__await__().send(None)  # Outside any loop
    assert stopped.value.value == "done"
    future = AwaitableFuture()
    future.cancel()
    with pytest.raises(asyncio.CancelledError):  # asyncio's, as wrap_future() would give, not concurrent.futures'
        future.__await__().send(None)


@pytest.mark.parametrize("method", ["result", "exception"])
def test_waiting_synchronously_on_its_own_loop_is_an_error(owner, method):
    async def main():
        future = AwaitableFuture(prevent_blocking_wait_in_loops=(owner, asyncio.get_running_loop()))
        with pytest.raises(RuntimeError, match="would block forever"):
            getattr(future, method)(0)
        with pytest.raises(TimeoutError):
            getattr(AwaitableFuture(prevent_blocking_wait_in_loops=(owner,)), method)(0.01)  # Not this loop: waits
        with pytest.raises(TimeoutError):
            getattr(AwaitableFuture(), method)(0.01)
        future.set_result("done")
        assert getattr(future, method)(0) == ("done" if method == "result" else None)  # Done, so it can't block

    asyncio.run(main())


def test_failure_is_what_a_done_future_failed_with(owner):
    pending, succeeded, failed = AwaitableFuture(), AwaitableFuture(), AwaitableFuture()
    succeeded.set_result("done")
    failed.set_exception(error := ValueError("failed"))
    assert pending.failure() is None and succeeded.failure() is None and failed.failure() is error
    cancelled = AwaitableFuture()
    cancelled.cancel()
    with pytest.raises(concurrent.futures.CancelledError):  # The fourth state: neither succeeded nor failed
        cancelled.failure()


def test_async_futures_are_one_per_loop(owner):
    future = AwaitableFuture()

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
        done, failed, cancelled = futures = [AwaitableFuture() for _ in range(3)]
        waiting = [future.as_async_future() for future in futures]
        for finish in lambda: done.set_result("done"), lambda: failed.set_exception(ValueError()), cancelled.cancel:
            threading.Thread(target=finish).start()  # From another thread, as a worker's would be
        results = await asyncio.gather(*waiting, return_exceptions=True)
        assert results[0] == "done" and type(results[1]) is ValueError and type(results[2]) is asyncio.CancelledError

    asyncio.run(main())


def test_cancelling_an_async_future_cancels_the_future(owner):
    async def main():
        future = AwaitableFuture()
        future.as_async_future().cancel()
        await asyncio.sleep(0)  # For the cancellation to reach it
        assert future.cancelled()

    asyncio.run(main())


def test_async_futures_need_a_running_loop_unless_told_otherwise(owner):
    future = AwaitableFuture()
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
        AwaitableFuture().as_async_future(owner)


@pytest.mark.xfail(strict=True, reason="BUG: a cached async future, once cancelled, is returned for good")
def test_an_async_future_cancelled_after_the_future_finished_isnt_returned_again(owner):
    async def main():
        future = AwaitableFuture()
        future.set_result("done")
        future.as_async_future().cancel()  # As its awaiter is cancelled before the result reaches it
        await asyncio.sleep(0)
        assert future.result() == "done"
        assert await future.as_async_future() == "done"

    asyncio.run(main())


@pytest.mark.parametrize(
    "awaitable",
    [
        pytest.param(lambda future: future, id="awaited"),
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
    """as_async_future() caches one asyncio.Future per loop, which all waiters there share, and cancelling a waiting task
    cancels the asyncio.Future it awaits. asyncio.wrap_future() passes that on to the source, through its
    concurrent.futures.Future.cancel(), which the source refuses if it's running: as an executor's job is, once started.
    The shared asyncio.Future is then cancelled though its source isn't, so one waiter giving up cancels it for every
    other waiter on that loop, now and later. Nothing in this package runs its futures (WorkerStopFuture, which refuses
    cancel() too, shields its waiters instead), so this stays an expected failure, to show the limit of the cache, and
    to flag a fix for it. Awaiting, which wraps the future afresh each time, is unaffected."""

    async def main():
        future = AwaitableFuture()
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


def test_cancelled_futures_count_as_done_for_concurrent_futures_wait_and_as_completed(owner):
    future = AwaitableFuture()
    threading.Timer(0.05, future.cancel).start()  # While concurrent.futures.wait() waits
    done, _ = concurrent.futures.wait([future], timeout=2)
    assert future in done
    assert future.cancel()  # Again, which is fine
    assert list(concurrent.futures.as_completed([future], timeout=0.1)) == [future]


def test_cancelling_from_several_threads_at_once_notifies_once(owner, caplog):
    for _ in range(20):
        future, barrier, outcomes = AwaitableFuture(), threading.Barrier(8), []

        def cancel():
            barrier.wait()
            try:
                outcomes.append(future.cancel())
            except BaseException as error:  # concurrent.futures raises RuntimeError on notifying twice
                outcomes.append(error)

        threads = [threading.Thread(target=cancel) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert outcomes == [True] * 8
    assert "unexpected state" not in caplog.text
