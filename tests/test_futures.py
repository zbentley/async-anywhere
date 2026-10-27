"""Tests for AwaitableFuture on its own, without a worker."""
from __future__ import annotations

import asyncio
import concurrent.futures
import gc
import threading
import time
import weakref

import anyio
import pytest
import trio

from async_anywhere.futures import AwaitableFuture


@pytest.fixture
def owner():
    """A loop that never runs."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


async def test_awaiting_works_on_any_loop():
    future = AwaitableFuture()

    async def wait():
        return await future

    elsewhere = asyncio.ensure_future(asyncio.to_thread(asyncio.run, wait()))
    here = asyncio.ensure_future(wait())
    while len(future._done_callbacks) < 2:  # noqa: ASYNC110 (Until both wait, through their loops' async futures, rather than find it done)
        await asyncio.sleep(0.001)
    threading.Thread(target=future.set_result, args=("done",)).start()
    assert await here == "done" and await elsewhere == "done"


def test_awaiting_a_done_future_needs_no_loop():
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
async def test_waiting_synchronously_on_its_own_loop_is_an_error(owner, method):
    future = AwaitableFuture(prevent_blocking_wait_in_loops=(owner, asyncio.get_running_loop()))
    with pytest.raises(RuntimeError, match="would deadlock"):
        getattr(future, method)(0)
    with pytest.raises(TimeoutError):
        getattr(AwaitableFuture(prevent_blocking_wait_in_loops=(owner,)), method)(0.01)  # Not this loop: waits
    with pytest.raises(TimeoutError):
        getattr(AwaitableFuture(), method)(0.01)
    future.set_result("done")
    assert getattr(future, method)(0) == ("done" if method == "result" else None)  # Done, so it can't block


@pytest.mark.parametrize("method", ["result", "exception"])
def test_waiting_synchronously_on_its_own_thread_is_an_error(method):
    other = threading.Thread()  # Never started
    future = AwaitableFuture(prevent_blocking_wait_in_threads=(other, threading.current_thread()))
    with pytest.raises(RuntimeError, match="would deadlock"):
        getattr(future, method)(0)
    with pytest.raises(TimeoutError):
        getattr(AwaitableFuture(prevent_blocking_wait_in_threads=other), method)(0.01)  # Not this thread: waits
    future.set_result("done")
    assert getattr(future, method)(0) == ("done" if method == "result" else None)  # Done, so it can't block


@pytest.mark.parametrize("collection", [list, set, iter], ids=["list", "set", "iterator"])
async def test_loops_and_threads_to_refuse_blocking_waits_in_can_be_any_iterable(collection):
    here, loop = threading.current_thread(), asyncio.get_running_loop()
    for future in (
        AwaitableFuture(prevent_blocking_wait_in_threads=collection([here])),
        AwaitableFuture(prevent_blocking_wait_in_loops=collection([loop])),
    ):
        with pytest.raises(RuntimeError, match="would deadlock"):
            future.result(0)


def test_futures_keep_the_loops_and_threads_they_refuse_blocking_waits_in_only_while_they_live():
    loop, thread = asyncio.new_event_loop(), threading.Thread()
    loop.close()
    future = AwaitableFuture(prevent_blocking_wait_in_loops=loop, prevent_blocking_wait_in_threads=thread)
    refs = [weakref.ref(loop), weakref.ref(thread)]
    del loop, thread, future
    gc.collect()
    assert all(ref() is None for ref in refs)


def test_failure_is_what_a_done_future_failed_with():
    pending, succeeded, failed = AwaitableFuture(), AwaitableFuture(), AwaitableFuture()
    succeeded.set_result("done")
    failed.set_exception(error := ValueError("failed"))
    assert pending.exception_or_none() is None and succeeded.exception_or_none() is None and failed.exception_or_none() is error
    cancelled = AwaitableFuture()
    cancelled.cancel()
    assert cancelled.exception_or_none() is None  # The fourth state: neither succeeded nor failed
    assert failed.result_or_none() is None and cancelled.result_or_none() is None  # Not raised, as result() would
    pending.raise_if_failed()
    succeeded.raise_if_failed()
    with pytest.raises(ValueError) as raised:
        failed.raise_if_failed()
    assert raised.value is error
    with pytest.raises(concurrent.futures.CancelledError):
        cancelled.raise_if_failed()


async def test_async_futures_are_on_the_given_or_running_loop():
    future = AwaitableFuture()
    here = future.as_async_future()
    assert here.get_loop() is future.as_async_future(asyncio.get_running_loop()).get_loop() is asyncio.get_running_loop()

    async def elsewhere():
        there = future.as_async_future()
        return there is not here and there.get_loop() is asyncio.get_running_loop()

    assert await asyncio.to_thread(asyncio.run, elsewhere())
    future.set_result("done")
    assert await here == "done"


async def test_async_futures_are_asyncio_futures():
    async_future = AwaitableFuture().as_async_future()
    assert asyncio.isfuture(async_future) and isinstance(async_future, asyncio.Future)
    assert asyncio.ensure_future(async_future) is async_future  # So APIs that take futures use it as it is


async def test_async_futures_follow_the_future():
    done, failed, cancelled = futures = [AwaitableFuture() for _ in range(3)]
    waiting = [future.as_async_future() for future in futures]
    for finish in lambda: done.set_result("done"), lambda: failed.set_exception(ValueError()), cancelled.cancel:
        threading.Thread(target=finish).start()  # From another thread, as a worker's would be
    results = await asyncio.gather(*waiting, return_exceptions=True)
    assert results[0] == "done" and type(results[1]) is ValueError and type(results[2]) is asyncio.CancelledError


async def test_cancelling_an_async_future_cancels_the_future():
    future = AwaitableFuture()
    future.as_async_future().cancel()
    assert future.cancelled()  # At once, so it can't finish first, leaving the async future cancelled though it isn't


def test_async_futures_need_a_running_loop_unless_told_otherwise():
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


async def test_an_async_future_cancelled_after_the_future_finished_isnt_returned_again():
    future = AwaitableFuture()
    future.set_result("done")
    assert not future.as_async_future().cancel()  # As its awaiter is cancelled before the result reaches it: refused
    await asyncio.sleep(0)
    assert future.result() == "done"
    assert await future.as_async_future() == "done"


@pytest.mark.parametrize(
    "awaitable", [lambda future: future, lambda future: future.as_async_future()], ids=["awaited", "as_async_future"]
)
async def test_an_awaiter_cancelled_as_the_future_finishes_leaves_other_waiters_alone(awaitable):
    future = AwaitableFuture()

    async def wait():
        return await awaitable(future)

    cancelled, other = asyncio.ensure_future(wait()), asyncio.ensure_future(wait())
    await asyncio.sleep(0)  # Both waiting, on the loop's one async future
    future.set_result("done")  # Which a callback, queued on the loop but not yet run, passes on to it
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    assert await other == "done"


@pytest.mark.parametrize(
    "awaitable", [lambda future: future, lambda future: future.as_async_future()], ids=["awaited", "as_async_future"]
)
async def test_giving_up_waiting_on_an_uncancellable_future_leaves_other_waiters_alone(awaitable):
    """Cancelling a waiting task cancels the asyncio.Future it awaits, which passes it on to the source, through its
    concurrent.futures.Future.cancel(), which the source refuses if it's running: as an executor's job is, once started.
    Only the giving-up waiter's own asyncio.Future is then cancelled."""
    future = AwaitableFuture()
    assert future.set_running_or_notify_cancel()  # Running, as an executor's job would be, so it can't be cancelled

    async def wait():
        return await awaitable(future)

    other = asyncio.ensure_future(wait())
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(wait(), 0.05)
    assert len(future._done_callbacks) == 1  # Only other's, as the one given up on was dropped
    future.set_result("done")
    assert await other == "done"
    assert await wait() == "done"


async def test_async_futures_finish_at_once_as_the_future_does_on_their_loop():
    future = AwaitableFuture()
    waiting = future.as_async_future()
    future.set_result("done")
    assert waiting.done()  # Not on a later loop iteration, as asyncio.wrap_future()'s would
    assert await waiting == "done"


async def test_a_finished_future_lets_go_of_its_async_futures():
    future = AwaitableFuture()
    waiting = weakref.ref(future.as_async_future())
    gc.disable()  # So that only refcounting frees it
    try:
        assert waiting() is not None  # Held by its done-callback while the future is pending, as asyncio.wrap_future()'s is
        future.set_result("done")
        assert waiting() is None  # Finished at once, on its loop, and dropped with the future's done-callbacks
    finally:
        gc.enable()


@pytest.mark.parametrize(
    ("method", "finish", "raises"),
    [
        ("result", "fail", ValueError),
        ("result", "fail while waiting", ValueError),
        ("result", "cancel", concurrent.futures.CancelledError),
        ("result", None, TimeoutError),
        ("exception", "cancel", concurrent.futures.CancelledError),
        ("exception", None, TimeoutError),
    ],
)
def test_what_it_raises_doesnt_hold_it(method, finish, raises):
    future, holder = AwaitableFuture(), []
    if finish == "fail":
        future.set_exception(ValueError())
    elif finish == "cancel":
        future.cancel()
    elif finish == "fail while waiting":
        holder.append(future)  # Popped as it's failed, so the timer's thread doesn't hold it either
        threading.Timer(0.01, lambda: holder.pop().set_exception(ValueError())).start()
    with pytest.raises(raises) as raised:
        getattr(future, method)(2 if finish else 0.01)
    freed = weakref.ref(future)
    gc.disable()  # So that only refcounting frees it, while what it raised, and so its traceback, lives on
    try:
        del future
        assert freed() is None and raised.value is not None
    finally:
        gc.enable()


@pytest.mark.parametrize("method", ["result", "exception"])
def test_an_infinite_timeout_waits_as_none_does(method):
    future = AwaitableFuture()
    threading.Timer(0.01, future.set_result, args=("done",)).start()
    assert getattr(future, method)(float("inf")) == ("done" if method == "result" else None)
    assert getattr(future, method)(float("inf")) == ("done" if method == "result" else None)  # Done, so at once


@pytest.mark.parametrize("method", ["result", "exception"])
def test_a_negative_infinite_timeout_times_out_at_once(method):
    with pytest.raises(TimeoutError):
        getattr(AwaitableFuture(), method)(float("-inf"))


def test_reprs_show_the_state_but_not_the_result():
    future = AwaitableFuture()
    assert "Pending" in repr(future)
    future.set_result("a result")
    assert "Succeeded[str]" in repr(future) and "a result" not in repr(future)


def test_cancelled_futures_count_as_done_for_concurrent_futures_wait_and_as_completed():
    future = AwaitableFuture()
    threading.Timer(0.05, future.cancel).start()  # While concurrent.futures.wait() waits
    done, _ = concurrent.futures.wait([future], timeout=2)
    assert future in done
    assert future.cancel()  # Again, which is fine
    assert list(concurrent.futures.as_completed([future], timeout=0.1)) == [future]


def test_executors_can_make_them(monkeypatch, caplog):
    monkeypatch.setattr(concurrent.futures._base, "Future", AwaitableFuture)  # What ThreadPoolExecutor.submit() makes
    release = threading.Event()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(release.wait)  # Holding up the rest
        succeeding, failing, cancelled = executor.submit(int, "1"), executor.submit(int, "x"), executor.submit(int, "2")
        assert isinstance(succeeding, AwaitableFuture) and cancelled.cancel()
        release.set()
        assert len(concurrent.futures.wait([succeeding, failing, cancelled], timeout=2).done) == 3
        assert succeeding.result() == 1 and type(failing.exception()) is ValueError
        assert list(executor.map(int, "34", timeout=2)) == [3, 4]  # So its worker skipped the cancelled one, and lived
    assert not any(record.name == "concurrent.futures" for record in caplog.records)


@pytest.mark.parametrize(
    "awaitable", [lambda future: future, lambda future: future.as_async_future()], ids=["awaited", "as_async_future"]
)
async def test_giving_up_waiting_on_a_future_that_doesnt_honor_cancel_leaves_other_waiters_alone(awaitable):
    # Unlike one that's running: see the expected failure above
    future = AwaitableFuture(honor_cancel=False)
    assert not future.cancel() and not future.cancelled()

    async def wait():
        return await awaitable(future)

    other = asyncio.ensure_future(wait())
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(wait(), 0.05)
    assert not future.cancelled()
    assert len(future._done_callbacks) == 1  # Only other's, as the one given up on was dropped
    future.set_result("done")
    assert await other == "done"
    assert await wait() == "done"


def test_completed_at_is_when_it_finished():
    done, failed, cancelled = futures = [AwaitableFuture() for _ in range(3)]
    seen = []
    for future in futures:
        assert future.completed_at is None
        future.add_done_callback(lambda finished: seen.append(finished.completed_at))
    before = time.monotonic()
    done.set_result("done")
    failed.set_exception(ValueError())
    cancelled.cancel()
    after = time.monotonic()
    assert all(before <= future.completed_at <= after for future in futures)
    assert seen == [future.completed_at for future in futures]  # Set before its callbacks run
    assert cancelled.cancel() and seen[2] == cancelled.completed_at  # Not again
    running = AwaitableFuture()
    assert running.set_running_or_notify_cancel()
    assert not running.cancel() and running.completed_at is None  # Refused, so it hasn't finished
    refusing = AwaitableFuture(honor_cancel=False)
    assert not refusing.cancel() and refusing.completed_at is None


# Other async libraries: awaited through anyio's API, so on trio as well as asyncio


@pytest.mark.backends("asyncio", "uvloop", "trio")
async def test_awaiting_works_with_any_library():
    done, failed = AwaitableFuture(), AwaitableFuture()
    results = []

    async def wait(future):
        try:
            results.append(await future)
        except ValueError as error:
            results.append(error)

    async with anyio.create_task_group() as group:
        group.start_soon(wait, done)
        group.start_soon(wait, failed)
        while not (done._done_callbacks and failed._done_callbacks):  # noqa: ASYNC110 (Until both wait, rather than find them done)
            await anyio.sleep(0.001)
        threading.Thread(target=done.set_result, args=("done",)).start()  # From another thread, as a worker's would be
        threading.Thread(target=failed.set_exception, args=(error := ValueError(),)).start()
    assert sorted(results, key=str) == [error, "done"]  # The same exception, raised again


@pytest.mark.backends("asyncio", "uvloop", "trio")
async def test_cancelling_a_waiter_cancels_the_future_with_any_library():
    future = AwaitableFuture()
    with anyio.move_on_after(0.01):
        await future
    assert future.cancelled()


@pytest.mark.backends("trio")
async def test_trio_futures_cancel_the_future():
    future = AwaitableFuture()
    here = future.as_async_future()
    assert not isinstance(here, asyncio.Future)
    assert here.cancel() and future.cancelled()
    with pytest.raises(concurrent.futures.CancelledError):  # As result() raises, not asyncio's
        await here
    with pytest.raises(concurrent.futures.CancelledError):  # Likewise, done, so awaited without its trio future
        await future


@pytest.mark.backends("trio")
async def test_giving_up_waiting_on_an_uncancellable_future_leaves_other_trio_waiters_alone():
    # As asyncio's waiters do: see the asyncio test above
    future = AwaitableFuture()
    assert future.set_running_or_notify_cancel()  # Running, as an executor's job would be, so it can't be cancelled
    results = []

    async def wait():
        results.append(await future)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(wait)
        with trio.move_on_after(0.01):
            await future
        assert not future.cancelled()
        assert len(future._done_callbacks) == 1  # Only the other waiter's, as the one given up on was dropped
        future.set_result("done")
    assert results == ["done"]
    assert await future == "done"


@pytest.mark.backends("trio")
async def test_giving_up_on_a_trio_future_leaves_its_other_awaits_alone():
    future = AwaitableFuture(honor_cancel=False)
    here, results = future.as_async_future(), []

    async def wait():
        results.append(await here)

    with trio.fail_after(2):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(wait)  # Another task awaiting the same trio future
            with trio.move_on_after(0.01):
                await here
            future.set_result("done")
        assert results == ["done"] and await here == "done"  # And awaiting it again, after giving up on it


@pytest.mark.backends("trio")
async def test_trio_futures_completion_time_is_their_sources_before_the_run_wakes_their_waiters():
    future = AwaitableFuture()
    here = future.as_async_future()
    future.set_result("done")
    assert here.completed_at is not None  # Though the run hasn't set its event yet
    assert await here == "done"


@pytest.mark.backends("asyncio")  # Not uvloop's, whose signal wakeup fd trio's run warns it collides with
async def test_awaiting_in_a_trio_run_inside_an_asyncio_task_waits_on_trio():
    # There, asyncio.current_task() is still the asyncio task that called trio.run(), blocking its loop
    future = AwaitableFuture()

    async def in_trio():
        return await future

    threading.Timer(0.01, future.set_result, args=("done",)).start()
    assert trio.run(in_trio) == "done"


def test_cancelling_from_several_threads_at_once_notifies_once(scale):
    for _ in range(20 * scale):
        future, barrier, outcomes = AwaitableFuture(), threading.Barrier(8 * scale), []

        def cancel(future=future, barrier=barrier, outcomes=outcomes):
            barrier.wait()
            outcomes.append(future.cancel())  # concurrent.futures raises RuntimeError on notifying twice

        threads = [threading.Thread(target=cancel) for _ in range(8 * scale)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert outcomes == [True] * 8 * scale
        assert future in concurrent.futures.wait([future], timeout=0).done  # Notified, which it counts as done
