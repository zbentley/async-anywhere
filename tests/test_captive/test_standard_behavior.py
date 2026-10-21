"""CaptiveWorker as it runs: constructing workers, bridging functions, and how calls compare with awaiting coroutines."""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import gc
import threading
import time
import traceback

import pytest

from async_anywhere.captive import CaptiveWorker
from async_anywhere.captive.exceptions import CleanStop, WorkerStopped
from async_anywhere.futures import AwaitableFuture
from tests.test_captive.utils import BlocksLoop, Tidy, Worker, bridge, running


# Lifecycle


@pytest.mark.parametrize("interrupt", [False, True])
async def test_idle_stop(interrupt):
    async with running() as worker:  # With the default parent task and hooks
        stopped = await worker.stop(interrupt=interrupt)
        assert type(stopped) is CleanStop and stopped.args == ("(idle) manually stopped",)


current = contextvars.ContextVar("current", default="unset")


class SeesContext(Worker):
    async def _run(self):
        self.mark("seen", current.get())
        await self.runloop


async def test_run_doesnt_see_the_constructors_context():
    current.set("constructor's")
    async with running(SeesContext) as worker:
        assert await worker.reached("seen") == "unset"


@pytest.mark.parametrize("interrupt", [False, True])
async def test_hooks_are_called_once_each_on_the_workers_thread(interrupt):
    async with running(Worker) as worker:
        await worker.stop(interrupt=interrupt)
        await worker.stop(interrupt=True)  # Again, which changes nothing
        assert await worker.reached("stopped") == (True, True, False)  # See Harness.on_stopped()
        # Awaited, as on_shutdown() runs just after stop()'s waiters wake
        assert await worker.reached("shutdown") == (True, True, True)
        assert not worker.reached("stopped", 2).done() and not worker.reached("shutdown", 2).done()


# A refused argument leaves the loop its stop futures made unclosed (attrs validates once every field is set): only the
# GC closes it, warning as it does
@pytest.mark.filterwarnings("ignore:unclosed event loop:ResourceWarning")
@pytest.mark.parametrize(
    "kwargs, error, match",
    [
        # Making a coroutine, but not an async def
        (dict(parent_task=lambda worker: asyncio.sleep(0)), TypeError, "^parent_task must be a coroutine function"),
        (dict(parent_task=print), TypeError, "^parent_task must be a coroutine function"),
        (dict(on_stopped=None), TypeError, "^'on_stopped' must be callable"),
        (dict(on_shutdown="on_shutdown"), TypeError, "^'on_shutdown' must be callable"),
        (dict(idle_timeout="1"), TypeError, "'idle_timeout' must be"),
        (dict(idle_timeout=0), ValueError, "^'idle_timeout' must be > 0"),
        (dict(loop_factory=3), TypeError, "callable"),  # Called as the stop futures are made, before attrs validates it
        (dict(loop_factory=lambda: "not a loop"), TypeError, "^loop_factory must make a new event loop"),
        (
            dict(loop_factory=lambda: (loop := asyncio.new_event_loop()).close() or loop),
            TypeError,
            "^loop_factory made a running or closed event loop",
        ),
    ],
)
def test_arguments_are_checked_as_workers_are_constructed(kwargs, error, match):
    threads = set(threading.enumerate())
    with pytest.raises(error, match=match):
        CaptiveWorker(**kwargs)
    assert set(threading.enumerate()) <= threads  # Refused before anything started


class Fetcher:
    """Code using a worker, which bridges its own methods to it."""

    def __init__(self, worker: CaptiveWorker):
        self.fetch = worker.bridge(self._fetch)

    async def _fetch(self, key):
        return key


async def test_functions_can_be_bridged_whenever():
    fetched = AwaitableFuture()

    async def parent_task(worker):
        fetched.set_result(await Fetcher(worker).fetch("at once"))  # Bridged, and called, as the worker starts
        await asyncio.Future()

    async with running(parent_task=parent_task) as worker:
        assert await fetched == "at once"
        assert await Fetcher(worker).fetch("later") == "later"  # Once the worker is running
        await worker.stop()
        refused = Fetcher(worker)  # Once it has stopped, which is allowed, though calls are refused
        with pytest.raises(CleanStop):
            await refused.fetch("too late")


def test_workers_are_slotted():
    with running() as worker:
        assert not hasattr(worker, "__dict__")  # Not unslotted, as attrs would leave it if it couldn't slot cached_property
        assert type(vars(CaptiveWorker)["_loop"]).__name__ == "member_descriptor" and worker._loop is worker._loop


async def test_the_parent_task_gets_the_worker_and_bridged_functions_just_their_callers_arguments():
    given = AwaitableFuture()

    async def parent_task(worker):
        given.set_result(worker)
        await asyncio.Future()

    async with running(parent_task=parent_task) as worker:
        assert worker._loop.is_running()  # Constructed only once it was
        assert await given is worker

        @worker.bridge
        async def free(key):
            return worker.in_worker(), key

        assert await free("y") == (True, "y") and free.__name__ == "free"
        assert await asyncio.to_thread(lambda: free("z").result(1)) == (True, "z")


# Calls compared with coroutines


class Records(Worker):
    def __init__(self):
        self.ran = []  # Before super().__init__(), which starts _run(): it might use this
        super().__init__()

    @bridge
    async def record(self, label):
        self.ran.append(label)
        self.mark(label)

    @bridge
    async def makes_but_never_awaits(self):
        self.record("in-loop")


async def test_calls_nobody_waits_on_run_all_the_same():
    # A coroutine never awaited would fail the test, as filterwarnings makes its RuntimeWarning an error
    async with running(Records) as worker:
        worker.record("from another thread")  # Dropped at once
        await worker.reached("from another thread")
        await worker.makes_but_never_awaits()
        await worker.reached("in-loop")
        assert worker.ran == ["from another thread", "in-loop"]


class Contexts(Worker):
    @bridge
    async def swap(self, value, until: AwaitableFuture | None = None):
        if until:
            await until
        seen = current.get()
        current.set(value)
        return seen

    @bridge
    async def swap_in_loop(self):
        current.set("outer")
        seen = await self.swap("inner")  # On the worker's own thread, a copy all the same
        return seen, current.get()

    @bridge
    async def smuggle(self, until: AwaitableFuture):
        current.set("smuggler's")
        # Made on the worker's own thread, and returned to another unawaited, still running as it's waited on
        return self.swap("set by the job", until)


async def test_calls_made_on_the_workers_thread_can_be_waited_on_elsewhere():
    async with running(Contexts) as worker:
        smuggled = await worker.smuggle(release := AwaitableFuture())
        assert not smuggled.done()
        release.set_result(None)
        assert await smuggled == "smuggler's"  # Run in a task of its own, with a copy of its caller's context
        smuggled = await worker.smuggle(release := AwaitableFuture())
        waited = asyncio.ensure_future(asyncio.to_thread(smuggled.result, 1))  # From a thread with no loop
        release.set_result(None)
        assert await waited == "smuggler's"


class WaitsOnItself(Worker):
    @bridge
    async def waits(self, method):
        return getattr(self.work(), method)(1)


@pytest.mark.parametrize("method", ["result", "exception"])
async def test_waiting_synchronously_on_the_workers_own_loop_is_an_error(method):
    async with running(WaitsOnItself) as worker:
        with pytest.raises(RuntimeError, match="would deadlock"):
            await worker.waits(method)


async def test_calls_made_once_the_worker_has_stopped_are_refused_unrun():
    # Refused, not forgotten: a coroutine never awaited would fail the test
    async with running(Records) as worker:
        await worker.stop()
        errors = []
        for label in "first", "second":
            with pytest.raises(CleanStop) as refused:
                await worker.record(label)
            errors.append(refused.value)
        assert worker.ran == []
        first, second = errors
        assert first is not second  # Copies: one shared exception would carry both raises' frames
        assert len(traceback.extract_tb(second.__traceback__)) == len(traceback.extract_tb(first.__traceback__))


class CleansUpWithACall(Worker):
    async def _run(self):
        try:
            await self.runloop
        except asyncio.CancelledError:
            try:
                await self.work()
            except WorkerStopped as refused:
                self.refused = refused
            raise


async def test_calls_on_the_workers_thread_are_refused_once_it_has_stopped_even_from_run():
    async with running(CleansUpWithACall) as worker:
        await worker.reached("_run")
        await worker.stop()
        assert type(worker.refused) is CleanStop  # So _run()'s cleanup must use the client directly


@pytest.mark.parametrize("wait", ["result", "exception"])
async def test_giving_up_a_synchronous_wait_leaves_the_call_running(wait):
    async with running(Worker) as worker:
        call = worker.waits_for(release := AwaitableFuture())
        with pytest.raises(TimeoutError):
            await asyncio.to_thread(getattr(call, wait), 0.05)
        release.set_result("finished")
        assert await asyncio.to_thread(call.result, 1) == "finished"


async def test_calls_can_be_had_as_asyncio_futures():
    async with running(Records) as worker:
        first, second = worker.record("first"), worker.record("second")
        done, _ = await asyncio.wait([asyncio.wrap_future(first), asyncio.wrap_future(second)])  # Takes only those
        assert len(done) == 2
        assert await first is None  # The same call, which ran once
        assert sorted(worker.ran) == ["first", "second"]


class AwaitsAsAsyncFuture(Worker):
    @bridge
    async def via_async_future(self):
        return await self.work(0.01, "inner").as_async_future()  # On the worker's own loop


async def test_calls_async_futures_work_on_any_loop_the_workers_included():
    async with running(AwaitsAsAsyncFuture) as worker:
        first, second = worker.work(0.01, "first"), worker.work(0.01, "second")
        done, _ = await asyncio.wait([first.as_async_future(), second.as_async_future()])
        assert done == {first.as_async_future(), second.as_async_future()}  # One per loop, so those asyncio.wait() took
        assert [await first, await second] == ["first", "second"]
        assert await worker.via_async_future() == "inner"

        async def elsewhere():
            return await worker.work(0.01, "elsewhere").as_async_future()

        assert await asyncio.to_thread(asyncio.run, elsewhere()) == "elsewhere"


async def test_calls_async_futures_are_cancelled_and_interrupted_as_awaiting_them_is():
    async with running(Tidy) as worker:
        worker.mark("release")  # Its cleanup need only happen
        cancelled, interrupted = worker.tidy(), worker.tidy()
        await worker.reached("tidy", 2)
        cancelled.as_async_future().cancel()
        await worker.reached("tidied")  # Its job got the cancellation, and cleaned up
        assert cancelled.cancelled()
        stopping = worker.stop(interrupt=True)
        with pytest.raises(
            concurrent.futures.CancelledError,
            check=lambda error: not isinstance(error, asyncio.CancelledError) and isinstance(error.__cause__, WorkerStopped),
        ):
            await interrupted.as_async_future()
        await stopping


def test_calls_cancelled_by_their_callers_count_as_done_for_concurrent_futures_wait():
    with running(Tidy) as worker:
        worker.mark("release")  # Its cleanup need only happen
        call = worker.tidy()
        worker.reached("tidy").result(2)
        threading.Timer(0.05, call.cancel).start()  # Cancelled while concurrent.futures.wait() waits
        started = time.monotonic()
        done, _ = concurrent.futures.wait([call], timeout=2)
        assert call in done and time.monotonic() - started < 1
        assert list(concurrent.futures.as_completed([call], timeout=0.1)) == [call]


async def test_calls_see_a_copy_of_the_callers_context_as_made():
    async with running(Contexts) as worker:
        current.set("as made")
        call = worker.swap("set by the job")
        current.set("as waited on")
        assert await call == "as made"
        assert current.get() == "as waited on"  # The job's change didn't reach us
        assert await worker.swap_in_loop() == ("outer", "outer")


def test_waiting_on_a_call_from_several_threads_at_once_gives_each_its_result(scale):
    with running(Worker) as worker:
        for _ in range(20 * scale):
            call, results = worker.waits_for(release := AwaitableFuture()), []

            def wait(call=call, results=results):
                results.append(call.result(10))

            threads = [threading.Thread(target=wait) for _ in range(8 * scale)]
            for thread in threads:
                thread.start()
            while len(call._condition._waiters) < len(threads):  # Released only once every thread is waiting
                time.sleep(0.001)
            release.set_result(result := object())
            for thread in threads:
                thread.join()
            assert results == [result] * len(threads)  # The same object for each


# Exceptions and cancellation


class RaisesCancelled(Worker):
    @bridge
    async def raise_cancelled(self):
        raise asyncio.CancelledError("raised by the job")

    @bridge
    async def via_nested(self):
        return await self.raise_cancelled()


@pytest.mark.parametrize("method", ["raise_cancelled", "via_nested"])
async def test_cancelled_error_raised_by_a_job_reaches_its_caller_as_an_error(method):
    async with running(RaisesCancelled) as worker:
        with pytest.raises(asyncio.CancelledError, match="raised by the job"):
            await getattr(worker, method)()
        assert asyncio.current_task().cancelling() == 0  # Raised, not a cancellation of this task
        with pytest.raises(asyncio.CancelledError, match="raised by the job"):
            await asyncio.to_thread(lambda: getattr(worker, method)().result(2))


async def test_cancelling_a_call_cancels_its_job_quietly():
    # Like a task cancelled and never awaited: the teardown fails the test if asyncio logs it as never retrieved
    async with running(Tidy) as worker:
        worker.mark("release")  # Its cleanup need only happen
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(worker.tidy(), 0.1)
        await worker.reached("tidied")  # The job got the cancellation, and cleaned up
        await worker.stop()


async def test_calls_cancelled_before_they_start_never_run():
    async with running(BlocksLoop) as worker:
        ran = []

        @worker.bridge
        async def record(label):
            ran.append(label)

        blocking = worker.blocks()
        await worker.reached("blocked")  # So the call can't start yet
        assert record("cancelled").cancel()
        worker.mark("release")
        await blocking
        await record("after")  # Queued behind the cancelled call
        assert ran == ["after"]


class FailsCleanup(Worker):
    @bridge
    async def failing_cleanup(self):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise ValueError("cleanup failed")


@pytest.mark.logs_asyncio_errors
async def test_cancelled_job_failing_in_cleanup_is_logged_as_in_asyncio(caplog):
    async with running(FailsCleanup) as worker:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(worker.failing_cleanup(), 0.1)
        await worker.stop()  # A drain, so once the job has finished failing
    gc.collect()
    assert "exception was never retrieved" in caplog.text and "cleanup failed" in caplog.text
