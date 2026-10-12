"""Tests for worker_p0's CaptiveWorker. Self-contained: plain test functions that each run their own event loop with
asyncio.run(), and no fixtures beyond pytest's own (caplog) and one autouse check below. Timings are short, so as to keep
the suite quick."""
import asyncio
import concurrent.futures
import contextlib
import contextvars
import gc
import os
import signal
import sys
import threading
import time
import traceback
import warnings

import pytest

from async_anywhere.captive import AwaitableFuture, CaptiveWorker, CleanStop, WorkerStopped

IDLE = 0.2  # The idle timeout of the workers that expire


class bridge:
    """Bridges a method of the test workers' with each worker's own bridge(), as worker.method(...)."""

    def __init__(self, func):
        self.func = func

    def __get__(self, worker, owner=None):
        return self if worker is None else worker.bridge(self.func)


@pytest.fixture(autouse=True)
def no_errors_in_callbacks(caplog):
    """Fails tests in which a worker's loop raised from a callback (as the idle timer does if it fires with calls in
    flight): asyncio only logs those."""
    yield
    errors = caplog.get_records("call")
    assert not [record for record in errors if record.getMessage().startswith("Exception in callback")]


async def stop(worker: CaptiveWorker, **kwargs) -> WorkerStopped:
    """Shuts `worker` down, and returns why it stopped."""
    with pytest.raises(WorkerStopped) as stopped:
        await worker.stop(**kwargs)
    return stopped.value


async def until(condition, timeout=2.0):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "Timed out waiting for a condition"
        await asyncio.sleep(0.01)


async def in_thread(function):
    """Runs a blocking `function` on a plain thread, returning what it returned or raised."""
    outcome = {}

    def run():
        try:
            outcome["result"] = function()
        except BaseException as error:
            outcome["error"] = error

    thread = threading.Thread(target=run)
    thread.start()
    await asyncio.to_thread(thread.join)
    return outcome


def reason(worker: CaptiveWorker) -> str:
    return worker._stopped.exception().args[0]


class Worker(CaptiveWorker):
    @bridge
    async def work(self, seconds=0.0, result="done"):
        await asyncio.sleep(seconds)
        return result

    @bridge
    async def nested(self, seconds):
        await self.work()  # Made on the worker's own thread, so this call's nested one ends long before it does
        await asyncio.sleep(seconds)
        return "outer done"


class Expiring(Worker):
    idle_timeout = IDLE


class SlowCleanup(Expiring):
    async def _run(self):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0.5)  # Longer than the idle timeout. Cancelling this again would cut it short.
            self.cleaned_up = True
            raise


class Tidy(Worker):
    @bridge
    async def tidy(self):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(0.2)  # Cleanup that awaits. Cancelling this again would cut it short.
            self.tidied = True
            raise


class Fails(Worker):
    async def _run(self):
        await asyncio.sleep(0.2)
        raise ValueError("boom")


class Returns(Worker):
    async def _run(self):
        await asyncio.sleep(0.2)


class StopsItself(Worker):
    async def _run(self):
        await asyncio.sleep(0.2)
        await self.stop(interrupt=True, wait=False)


VAR = contextvars.ContextVar("VAR", default="unset")


class SeesContext(Worker):
    async def _run(self):
        self.seen = VAR.get()
        await asyncio.Event().wait()


# Lifecycle


@pytest.mark.parametrize("interrupt", [False, True])
def test_idle_stop(interrupt):
    async def main():
        worker = Worker()
        stopped = await stop(worker, interrupt=interrupt)
        assert type(stopped) is CleanStop and stopped.args == ("(idle) manually stopped",)
        worker._thread.join(1)
        assert not worker._thread.is_alive()

    asyncio.run(main())


def test_expires_when_idle():
    async def main():
        worker = Expiring()
        await until(lambda: worker._stopped.done())
        assert type(worker._stopped.exception()) is CleanStop and reason(worker) == "(idle) expired"
        worker._thread.join(1)
        assert not worker._thread.is_alive()

    asyncio.run(main())


def test_countdown_restarts_after_the_last_call():
    async def main():
        worker = Expiring()
        await asyncio.sleep(IDLE / 2)
        await worker.work()
        started = time.monotonic()
        await until(lambda: worker._stopped.done())
        assert IDLE / 2 < time.monotonic() - started < IDLE * 3

    asyncio.run(main())


def test_calls_in_flight_hold_off_expiry():
    async def main():
        worker = Expiring()
        assert await worker.work(IDLE * 1.5, "alone") == "alone"
        short, long = asyncio.ensure_future(worker.work(0.05)), asyncio.ensure_future(worker.work(IDLE * 3, "long"))
        assert await short == "done"
        assert await long == "long"  # Not cut short by expiry, though the short call ended long before it
        assert not worker._stopped.done()
        await until(lambda: worker._stopped.done())
        assert reason(worker) == "(idle) expired"

    asyncio.run(main())


def test_nested_call_holds_off_expiry_until_the_outer_one_ends():
    async def main():
        worker = Expiring()
        assert await worker.nested(IDLE * 3) == "outer done"
        assert not worker._stopped.done()
        await stop(worker)

    asyncio.run(main())


class CallsFromRun(Expiring):
    async def _run(self):
        self.result = await self.work(IDLE * 3, "from _run()")
        await asyncio.Event().wait()


def test_call_from_run_holds_off_expiry():
    async def main():
        worker = CallsFromRun()
        await until(lambda: hasattr(worker, "result"))
        assert worker.result == "from _run()" and not worker._stopped.done()
        await until(lambda: worker._stopped.done())
        assert reason(worker) == "(idle) expired"

    asyncio.run(main())


class Busy(Expiring):
    async def _run(self):
        try:
            while True:
                await asyncio.sleep(IDLE / 4)
                self._reschedule()  # Activity of its own
        except asyncio.CancelledError:
            for _ in range(8):
                await asyncio.sleep(IDLE / 4)
                self._reschedule()  # Mustn't restart the countdown now, which would cancel this cleanup
            self.cleaned_up = True
            raise


def test_run_can_hold_off_expiry_with_activity_of_its_own():
    async def main():
        worker = Busy()
        await asyncio.sleep(IDLE * 3)
        assert not worker._stopped.done()
        await stop(worker)
        assert worker.cleaned_up

    asyncio.run(main())


def test_calls_during_expiry_cleanup_are_refused():
    async def main():
        worker = SlowCleanup()
        await until(lambda: worker._stopped.done())
        with pytest.raises(CleanStop, match=r"\(idle\) expired"):
            await worker.work()
        assert not hasattr(worker, "cleaned_up")  # Refused while _run() was still cleaning up
        await stop(worker)
        assert worker.cleaned_up

    asyncio.run(main())


@pytest.mark.parametrize(
    "cls, kind, text",
    [
        (Returns, CleanStop, "(interrupted) _run() returned"),
        (Fails, WorkerStopped, "(interrupted) _run() raised ValueError('boom')"),
        (StopsItself, WorkerStopped, "(interrupted) manually stopped"),
    ],
)
def test_run_ending_stops_the_worker(cls, kind, text):
    async def main():
        worker = cls()
        with pytest.raises(concurrent.futures.CancelledError):
            await worker.work(10)  # In flight as _run() ends, so interrupted
        assert type(worker._stopped.exception()) is kind and reason(worker) == text
        worker._thread.join(1)
        assert not worker._thread.is_alive()

    asyncio.run(main())


def test_run_doesnt_see_the_constructors_context():
    async def main():
        VAR.set("constructor's")
        worker = SeesContext()
        await until(lambda: hasattr(worker, "seen"))
        assert worker.seen == "unset"
        await stop(worker)

    asyncio.run(main())


class Hooked(Worker):
    def __init__(self):
        self.hooks = []
        super().__init__()

    def on_stop_requested(self):
        self.hooks.append(("stop requested", self.in_worker, self._stopped.done(), self._finished.done()))

    def on_shutdown(self):
        self.hooks.append(("shutdown", self.in_worker, self._finished.done(), self._loop.is_closed()))


@pytest.mark.parametrize("interrupt", [False, True])
def test_hooks_are_called_once_each_on_the_workers_thread(interrupt):
    async def main():
        worker = Hooked()
        await asyncio.sleep(0.01)
        await stop(worker, interrupt=interrupt)
        await stop(worker, interrupt=True)  # Again, which changes nothing
        worker._thread.join(1)  # As on_shutdown() runs just after stop()'s waiters wake
        assert worker.hooks == [("stop requested", True, True, False), ("shutdown", True, True, True)]

    asyncio.run(main())


class Fetches(CaptiveWorker):
    def __init__(self):
        self.fetch = self.bridge(self._fetch)  # Before super().__init__(), which starts _run(): it might use it
        super().__init__()

    async def _fetch(self, worker, key):
        return worker is self, key


def test_bridging_functions_and_the_workers_own_methods():
    async def main():
        worker = Fetches()

        @worker.bridge
        async def free(worker, key):
            return worker.in_worker, key

        assert await worker.fetch("x") == (True, "x")
        assert await free("y") == (True, "y") and free.__name__ == "free"
        assert await asyncio.to_thread(lambda: free("z").result(1)) == (True, "z")
        await stop(worker)

    asyncio.run(main())

# Interrupts


def test_interrupt_cancels_calls_once_and_tells_callers_why():
    async def main():
        worker = Tidy()
        call = asyncio.ensure_future(worker.tidy())
        await asyncio.sleep(0.05)
        stopped = await stop(worker, interrupt=True)
        assert type(stopped) is WorkerStopped and stopped.args == ("(interrupted) manually stopped",)
        assert worker.tidied  # Its cleanup wasn't cut short
        with pytest.raises(concurrent.futures.CancelledError) as raised:
            await call
        error = raised.value
        assert not isinstance(error, asyncio.CancelledError)  # Which asyncio would take for this task being cancelled
        assert "tidy" in [frame.name for frame in traceback.extract_tb(error.__traceback__)]  # Where it was cancelled
        why = error.__cause__
        assert type(why) is WorkerStopped and why.args == stopped.args and why is not stopped  # A copy
        assert why.__cause__ is None and stopped.__cause__ is None

    asyncio.run(main())


def test_interrupted_callers_see_what_stopped_the_worker():
    async def main():
        worker = Fails()
        outcome = await in_thread(lambda: worker.work(10).result(5))
        error = outcome["error"]
        assert isinstance(error, concurrent.futures.CancelledError)
        why = error.__cause__
        assert type(why) is WorkerStopped and why.args == (reason(worker),)
        assert type(why.__cause__) is ValueError and why.__cause__.args == ("boom",)

    asyncio.run(main())


class RunAwaitsACall(Worker):
    async def _run(self):
        try:
            await self.work(10)
        except BaseException as error:
            self.raised = error
            raise


def test_interrupted_calls_on_the_workers_thread_are_cancelled_as_usual():
    async def main():
        worker = RunAwaitsACall()
        await asyncio.sleep(0.05)
        await stop(worker, interrupt=True)
        assert type(worker.raised) is asyncio.CancelledError  # Not converted, as for callers on other threads

    asyncio.run(main())


class Supervises(Worker):
    def __init__(self):
        self.events = []
        super().__init__()

    async def _run(self):
        async with asyncio.TaskGroup() as group:
            for _ in range(3):
                group.create_task(self.child())
            await asyncio.Event().wait()

    async def child(self):
        await self.cleans_up()  # So its cancellation, by _run()'s TaskGroup as it's interrupted, cancels the call too

    @bridge
    async def cleans_up(self):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            try:
                await asyncio.sleep(0.01)
                self.events.append("cleaned up")
            except asyncio.CancelledError:
                self.events.append("cut short")
            raise


def test_an_interrupt_cancels_calls_once_though_what_awaits_them_is_cancelled_too():
    # Cancelled by the interrupt and again through what awaits them, calls cleaning up would be cut short
    workers = [Supervises() for _ in range(10)]
    time.sleep(0.05)
    for worker in workers:
        with pytest.raises(WorkerStopped):
            worker.stop(interrupt=True).result(2)
    assert [event for worker in workers for event in worker.events] == ["cleaned up"] * 30


class LeavesACall(Worker):
    async def _run(self):
        self.left = self.waits()  # Not awaited, so nothing but the worker cancels it
        await asyncio.Event().wait()

    @bridge
    async def waits(self):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            self.cancelled = asyncio.current_task().cancelling()
            raise


def test_calls_on_the_workers_thread_are_interrupted_like_any_other():
    async def main():
        worker = LeavesACall()
        await asyncio.sleep(0.05)
        await stop(worker, interrupt=True)
        assert worker.cancelled == 1
        assert isinstance(worker.left.exception(1).__cause__, WorkerStopped)  # Interrupted, and told why

    asyncio.run(main())


class CancelsWhatItLeaves(Worker):
    async def _run(self):
        async def call():
            await self.cleans_up()  # So cancelling this task cancels the call too, unless the worker already has

        self.left = asyncio.create_task(call())
        try:
            await asyncio.Event().wait()
        finally:
            self.left.cancel()
            await asyncio.sleep(0)  # Into the call's cleanup, which _run() doesn't wait for

    @bridge
    async def cleans_up(self):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            try:
                await asyncio.sleep(0.01)
                self.cleaned_up = True
            except asyncio.CancelledError:
                self.cleaned_up = False  # Cancelled again
            raise


def test_calls_on_the_workers_thread_that_run_has_cancelled_arent_cancelled_again():
    async def main():
        worker = CancelsWhatItLeaves()
        await asyncio.sleep(0.05)
        await stop(worker, interrupt=True)
        assert worker.cleaned_up

    asyncio.run(main())


class Exits(Worker):
    @bridge
    async def exit(self):
        await asyncio.sleep(0.05)
        sys.exit(3)

    @bridge
    async def slow(self):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(0.01)  # Within the teardown's 0.05s
            self.cleaned_up = True
            raise


def test_a_crash_stops_the_worker_and_interrupts_calls_in_flight(caplog):
    crashes, hook = [], threading.excepthook
    threading.excepthook = lambda args: crashes.append(args.exc_type)
    try:

        async def main():
            worker = Exits()
            slow = asyncio.ensure_future(worker.slow())
            exiting = await in_thread(lambda: worker.exit().result(2))
            assert type(exiting["error"]) is SystemExit  # Raised again in its caller, as awaiting it in-loop would
            with pytest.raises(concurrent.futures.CancelledError) as raised:
                await slow
            why = raised.value.__cause__
            assert why.args == ("(interrupted) the worker's event loop crashed",) and type(why.__cause__) is SystemExit
            assert worker.cleaned_up  # Cancelled once, by the teardown
            worker._thread.join(1)
            assert (await stop(worker)).args == why.args

        asyncio.run(main())
    finally:
        threading.excepthook = hook
    assert crashes == [SystemExit]  # Raised again on the worker's thread
    assert caplog.text.count("Stopping due to error") == 1


def test_interrupting_during_expiry_cleanup_cancels_run_again():
    async def main():
        worker = SlowCleanup()
        await until(lambda: worker._stopped.done())
        stopped = await stop(worker, interrupt=True)
        assert stopped.args == ("(idle) expired",) and worker._interrupted
        assert not hasattr(worker, "cleaned_up")  # Cut short

    asyncio.run(main())


class Endures(Worker):
    """Its _run() and calls carry on through their first cancellation, ending at their second."""

    def __init__(self):
        self.cancels = {"_run": 0, "call": 0}
        super().__init__()

    async def _run(self):
        await self.endure("_run")

    @bridge
    async def call(self):
        await self.endure("call")

    async def endure(self, who):
        while True:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                self.cancels[who] += 1
                if self.cancels[who] == 2:
                    raise


def test_each_interrupting_stop_cancels_run_and_calls_again():
    async def main():
        worker = Endures()
        call = worker.call()
        await asyncio.sleep(0.05)
        with pytest.raises(WorkerStopped):
            await worker.stop(interrupt=True, wait=False)
        await until(lambda: worker.cancels == {"_run": 1, "call": 1})
        with pytest.raises(WorkerStopped):
            await worker.stop(wait=False)  # Doesn't interrupt, so cancels nothing
        await asyncio.sleep(0.05)
        assert worker.cancels == {"_run": 1, "call": 1}
        await stop(worker, interrupt=True)
        assert worker.cancels == {"_run": 2, "call": 2}
        with pytest.raises(concurrent.futures.CancelledError):
            await call

    asyncio.run(main())


def test_a_callers_cancellation_after_an_interrupt_doesnt_cancel_the_call_again():
    async def main():
        worker = Tidy()
        call = worker.tidy()
        await asyncio.sleep(0.05)
        with pytest.raises(WorkerStopped):
            await worker.stop(interrupt=True, wait=False)
        assert call.cancel()  # While it's cleaning up
        await stop(worker)
        assert worker.tidied  # Not cut short

    asyncio.run(main())


@pytest.mark.parametrize("interrupt", [False, True])
def test_stop_cleanup_outlasting_the_idle_timeout_isnt_cut_short(interrupt):
    async def main():
        worker = SlowCleanup()
        await stop(worker, interrupt=interrupt)
        assert worker.cleaned_up

    asyncio.run(main())


class WithHelper(Worker):
    async def _run(self):
        async def helper():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0.2)
                self.helper_cleaned_up = True
                raise

        async with asyncio.TaskGroup() as group:
            group.create_task(helper())


@pytest.mark.parametrize("interrupt", [False, True])
def test_runs_own_tasks_are_left_to_it(interrupt):
    async def main():
        worker = WithHelper()
        await asyncio.sleep(0.05)
        await stop(worker, interrupt=interrupt)
        assert worker.helper_cleaned_up  # Cancelled once, by _run()'s task group, and not again by the worker

    asyncio.run(main())


class Retries(Worker):
    @bridge
    async def retry_forever(self):
        while True:
            try:
                async with asyncio.timeout(0.1):
                    try:
                        await asyncio.sleep(10)
                    except asyncio.CancelledError:
                        await asyncio.sleep(0.3)  # Still inside the timeout, which is expiring, as the interrupt lands
                        raise
            except TimeoutError:
                pass


def test_interrupt_isnt_lost_to_a_timeout_inside_the_call():
    async def main():
        worker = Retries()
        call = asyncio.ensure_future(worker.retry_forever())
        await asyncio.sleep(0.2)
        await asyncio.wait_for(stop(worker, interrupt=True), 3)
        with pytest.raises(concurrent.futures.CancelledError):
            await call

    asyncio.run(main())


# Drains


def test_drain_refuses_new_calls_and_lets_calls_in_flight_finish():
    async def main():
        worker = Worker()
        call = asyncio.ensure_future(worker.work(0.3, "finished"))
        await asyncio.sleep(0.05)
        started = time.monotonic()
        with pytest.raises(CleanStop, match=r"\(waiting for running jobs\) manually stopped"):
            await worker.stop(wait=False)
        assert time.monotonic() - started < 0.1  # Only until the stop began
        with pytest.raises(CleanStop):
            await worker.work()
        assert await call == "finished"
        await stop(worker)
        worker._thread.join(1)
        assert not worker._thread.is_alive() and worker._loop.is_closed()

    asyncio.run(main())


class BlocksLoop(Worker):
    @bridge
    async def blocks(self):
        await asyncio.sleep(0.05)
        time.sleep(0.5)  # Blocking the worker's loop, synchronously


def test_calls_made_during_a_drain_are_refused_without_waiting_for_the_loop():
    async def main():
        worker = BlocksLoop()
        blocking = asyncio.ensure_future(worker.blocks())
        await asyncio.sleep(0.02)
        with pytest.raises(CleanStop, match="waiting for running jobs"):
            await worker.stop(wait=False)
        await asyncio.sleep(0.1)  # Its loop is blocked now
        started = time.monotonic()
        refused = (await in_thread(lambda: worker.work().exception(0.3)))["result"]
        assert type(refused) is CleanStop and time.monotonic() - started < 0.2  # Rather than a TimeoutError, after 0.3s
        await blocking
        await stop(worker)

    asyncio.run(main())


def test_abandoned_drain_keeps_going_and_can_be_cut_short():
    async def main():
        worker = Worker()
        call = asyncio.ensure_future(worker.work(0.3, "finished"))
        await asyncio.sleep(0.05)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(worker.stop(), 0.1)
        assert await call == "finished"  # The drain went on regardless
        await stop(worker)

        worker = Worker()
        call = asyncio.ensure_future(worker.work(10))
        await asyncio.sleep(0.05)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(worker.stop(), 0.1)
        stopped = await stop(worker, interrupt=True)
        assert stopped.args == ("(waiting for running jobs) manually stopped",)  # The first stop's reason stands
        with pytest.raises(concurrent.futures.CancelledError):
            await call

    asyncio.run(main())


def test_draining_caller_gets_why_when_another_stop_finishes_the_worker():
    async def main():
        for _ in range(10):
            worker = Worker()
            call = asyncio.ensure_future(worker.work(5))
            await asyncio.sleep(0.02)
            drain = asyncio.ensure_future(worker.stop())
            await asyncio.sleep(0.02)
            await stop(worker, interrupt=True)
            with pytest.raises(CleanStop):  # Not a CancelledError, though the teardown may cancel the drain's stop
                await drain
            with pytest.raises(concurrent.futures.CancelledError):
                await call

    asyncio.run(main())


class ReturnsWhenStopped(Worker):
    async def _run(self):
        with contextlib.suppress(WorkerStopped):
            await self._stopped  # Returns as soon as the worker stops, rather than waiting to be cancelled


def test_a_drain_outlasts_run_returning():
    async def main():
        worker = ReturnsWhenStopped()
        call = worker.work(0.3, "finished")
        await asyncio.sleep(0.05)
        stopped = await stop(worker)
        assert stopped.args == ("(waiting for running jobs) manually stopped",)
        assert await call == "finished"  # Not interrupted, as _run() ended after the stop, not of its own accord

    asyncio.run(main())


class RunCalls(Worker):
    async def _run(self):
        await self.work(0.3)
        await asyncio.Event().wait()


def test_drain_ends_when_runs_own_call_does():
    async def main():
        worker = RunCalls()
        await asyncio.sleep(0.1)
        stopped = await asyncio.wait_for(stop(worker), 2)
        assert stopped.args == ("(waiting for running jobs) manually stopped",)

    asyncio.run(main())


class Spawner(Expiring):
    @bridge
    async def spawn(self, seconds):
        asyncio.get_running_loop().create_task(asyncio.sleep(seconds))


def test_tasks_calls_leave_running_dont_hold_anything_up():
    async def main():
        worker = Spawner()
        await worker.spawn(10)
        started = time.monotonic()
        await until(lambda: worker._stopped.done())
        assert time.monotonic() - started < IDLE * 3  # Expired without waiting for that task

        worker = Spawner()
        await worker.spawn(10)
        started = time.monotonic()
        await stop(worker)  # A drain
        assert time.monotonic() - started < 0.5

    asyncio.run(main())


# stop()


class Stops(Worker):
    @bridge
    async def stop_waiting(self):
        self.stop()  # wait=True, on the worker's own thread

    @bridge
    async def stop_interrupting(self):
        self.stop(interrupt=True, wait=False)
        await asyncio.sleep(10)

    @bridge
    async def drain_from_inside(self):
        stopped = self.stop(wait=False).exception()  # Already done: the stop began as stop() was called
        await asyncio.sleep(0.2)  # This call may still finish
        return stopped


def test_stop_cant_wait_on_the_workers_own_thread():
    async def main():
        worker = Stops()
        with pytest.raises(AssertionError, match="would wait for itself"):
            await worker.stop_waiting()
        assert not worker._stopped.done()
        await stop(worker)

    asyncio.run(main())


def test_interrupting_from_inside_a_call_cancels_it_too():
    async def main():
        worker = Stops()
        other = asyncio.ensure_future(worker.work(10))
        await asyncio.sleep(0.05)
        with pytest.raises(concurrent.futures.CancelledError):
            await worker.stop_interrupting()
        with pytest.raises(concurrent.futures.CancelledError):
            await other
        assert reason(worker) == "(interrupted) manually stopped"
        await stop(worker)

    asyncio.run(main())


def test_draining_from_inside_a_call_returns_at_once_and_lets_calls_finish():
    async def main():
        worker = Stops()
        other = asyncio.ensure_future(worker.work(0.4, "finished"))
        await asyncio.sleep(0.05)
        stopped = await worker.drain_from_inside()
        assert type(stopped) is CleanStop and stopped.args == ("(waiting for running jobs) manually stopped",)
        assert await other == "finished"
        await stop(worker)

    asyncio.run(main())


def test_stop_can_be_waited_on_from_a_plain_thread():
    async def main():
        worker = Worker()
        outcome = await in_thread(lambda: worker.stop().result(2))
        assert type(outcome["error"]) is CleanStop and outcome["error"].args == ("(idle) manually stopped",)
        worker._thread.join(1)
        assert not worker._thread.is_alive()

    asyncio.run(main())


def test_giving_up_waiting_on_stop_doesnt_cancel_the_stop():
    async def main():
        worker = Worker()
        with pytest.raises((TimeoutError, CleanStop)):
            worker.stop(wait=False).result(timeout=0)
        await until(lambda: worker._stopped.done())
        assert not worker._stopped.cancelled()
        await stop(worker)

    asyncio.run(main())


class SlowStop(Worker):
    async def _run(self):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0.5)
            raise


def test_waiting_for_stop_doesnt_block_the_callers_loop():
    async def main():
        ticks = 0

        async def tick():
            nonlocal ticks
            while True:
                ticks += 1
                await asyncio.sleep(0.005)

        ticker = asyncio.ensure_future(tick())
        await stop(SlowStop())
        ticker.cancel()
        assert ticks > 30  # About 100, if never blocked

    asyncio.run(main())


def test_stop_future_can_be_waited_on_anywhere_and_not_cancelled(caplog):
    async def main():
        worker = Worker()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(worker._stopped, 0.05)  # Gives up, which mustn't cancel the stop
        assert not worker._stopped.cancelled()

        def on_another_loop():
            async def wait():
                await worker._stopped

            asyncio.run(wait())

        blocked = asyncio.ensure_future(in_thread(lambda: worker._stopped.result(2)))
        blocked_too = asyncio.ensure_future(in_thread(lambda: worker._stopped.result(2)))
        awaited = asyncio.ensure_future(in_thread(on_another_loop))
        await asyncio.sleep(0.05)
        stopped = await stop(worker)
        errors = [(await blocked)["error"], (await blocked_too)["error"], (await awaited)["error"], stopped]
        assert all(type(error) is CleanStop and error.args == stopped.args for error in errors)
        assert len({id(error) for error in errors}) == 4  # Each a copy
        first, second = (traceback.extract_tb(error.__traceback__) for error in errors[:2])
        assert len(first) == len(second)  # Neither carries the other's frames
        gc.collect()

    asyncio.run(main())
    assert "shielded future" not in caplog.text and "never retrieved" not in caplog.text


# Calls compared with coroutines


class Records(Worker):
    def __init__(self):
        self.ran = []  # Before super().__init__(), which starts _run(): it might use this
        super().__init__()

    @bridge
    async def record(self, label):
        self.ran.append(label)

    @bridge
    async def makes_but_never_awaits(self):
        self.record("in-loop")


def test_calls_nobody_waits_on_run_all_the_same():
    async def main():
        worker = Records()
        worker.record("from another thread")  # Dropped at once
        await until(lambda: worker.ran == ["from another thread"])
        await worker.makes_but_never_awaits()
        await until(lambda: worker.ran == ["from another thread", "in-loop"])
        await stop(worker)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        asyncio.run(main())
        gc.collect()
    assert not [warning for warning in caught if "never awaited" in str(warning.message)]


class Smuggles(Worker):
    @bridge
    async def smuggle(self):
        current.set("smuggler's")
        return self.swap("set by the job")  # Made on the worker's own thread, and returned to another unawaited

    @bridge
    async def swap(self, value):
        await asyncio.sleep(0.2)  # Still running as it's waited on
        seen = current.get()
        current.set(value)
        return seen


def test_calls_made_on_the_workers_thread_can_be_waited_on_elsewhere():
    async def main():
        worker = Smuggles()
        smuggled = await worker.smuggle()
        assert not smuggled.done()
        assert await smuggled == "smuggler's"  # Run in a task of its own, with a copy of its caller's context
        smuggled = await worker.smuggle()
        assert (await in_thread(lambda: smuggled.result(1)))["result"] == "smuggler's"  # From a thread with no loop
        await stop(worker)

    asyncio.run(main())


class WaitsOnItself(Worker):
    @bridge
    async def waits(self, method):
        return getattr(self.work(), method)(1)


@pytest.mark.parametrize("method", ["result", "exception"])
def test_waiting_synchronously_on_the_workers_own_loop_is_an_error(method):
    async def main():
        worker = WaitsOnItself()
        with pytest.raises(RuntimeError, match="would block forever"):
            await worker.waits(method)
        await stop(worker)

    asyncio.run(main())


def test_calls_made_once_the_worker_has_stopped_are_refused_unrun():
    async def main():
        worker = Records()
        await stop(worker)
        errors = []
        for label in "first", "second":
            with pytest.raises(CleanStop) as refused:
                await worker.record(label)
            errors.append(refused.value)
        assert worker.ran == []
        first, second = errors
        assert first is not second  # Copies: one shared exception would carry both raises' frames
        assert len(traceback.extract_tb(second.__traceback__)) == len(traceback.extract_tb(first.__traceback__))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        asyncio.run(main())
        gc.collect()
    assert not [warning for warning in caught if "never awaited" in str(warning.message)]  # Refused, not forgotten


def test_calls_made_before_a_stop_run_though_waited_on_after_it():
    async def main():
        worker = Records()
        call = worker.record("before")
        await until(lambda: worker.ran)  # Started: one made just before the stop, but not yet started, would be refused
        await stop(worker)
        assert await call is None and worker.ran == ["before"]

    asyncio.run(main())


class CleansUpWithACall(Worker):
    async def _run(self):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            try:
                await self.work()
            except WorkerStopped as refused:
                self.refused = refused
            raise


def test_calls_on_the_workers_thread_are_refused_once_it_has_stopped_even_from_run():
    async def main():
        worker = CleansUpWithACall()
        await asyncio.sleep(0.05)
        await stop(worker)
        assert type(worker.refused) is CleanStop  # So _run()'s cleanup must use the client directly

    asyncio.run(main())


@pytest.mark.parametrize("wait", ["result", "exception"])
def test_giving_up_a_synchronous_wait_leaves_the_call_running(wait):
    async def main():
        worker = Worker()
        call = worker.work(0.2, "finished")
        outcome = await in_thread(lambda: getattr(call, wait)(0.05))
        assert type(outcome["error"]) is TimeoutError
        assert (await in_thread(lambda: call.result(1)))["result"] == "finished"
        await stop(worker)

    asyncio.run(main())


def test_calls_can_be_had_as_asyncio_futures():
    async def main():
        worker = Records()
        first, second = worker.record("first"), worker.record("second")
        done, _ = await asyncio.wait([asyncio.wrap_future(first), asyncio.wrap_future(second)])  # Takes only those
        assert len(done) == 2 and sorted(worker.ran) == ["first", "second"]
        assert await first is None  # The same call, which ran once
        assert sorted(worker.ran) == ["first", "second"]
        await stop(worker)

    asyncio.run(main())


def test_calls_made_together_run_together():
    async def main():
        worker = Worker()
        started = time.monotonic()
        first, second = worker.work(0.2), worker.work(0.2)
        await first
        await second
        assert time.monotonic() - started < 0.35  # Both started as made, not as awaited
        await stop(worker)

    asyncio.run(main())


current = contextvars.ContextVar("current", default="unset")


class Contexts(Worker):
    @bridge
    async def swap(self, value):
        seen = current.get()
        current.set(value)
        return seen

    @bridge
    async def swap_in_loop(self):
        current.set("outer")
        seen = await self.swap("inner")  # On the worker's own thread, a copy all the same
        return seen, current.get()


def test_calls_see_a_copy_of_the_callers_context_as_made():
    async def main():
        worker = Contexts()
        current.set("as made")
        call = worker.swap("set by the job")
        current.set("as waited on")
        assert await call == "as made"
        assert current.get() == "as waited on"  # The job's change didn't reach us
        assert await worker.swap_in_loop() == ("outer", "outer")
        await stop(worker)

    asyncio.run(main())


class Counts(Worker):
    def __init__(self):
        self.runs = 0
        super().__init__()

    @bridge
    async def count(self):
        self.runs += 1
        await asyncio.sleep(0.02)


def test_waiting_on_a_call_from_several_threads_at_once_runs_it_once():
    async def main():
        worker = Counts()
        for _ in range(20):
            call, barrier = worker.count(), threading.Barrier(8)

            def wait():
                barrier.wait()
                call.result(2)

            threads = [threading.Thread(target=wait) for _ in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                await asyncio.to_thread(thread.join)
        assert worker.runs == 20
        await stop(worker)

    asyncio.run(main())


# Exceptions and cancellation


class RaisesCancelled(Worker):
    @bridge
    async def raise_cancelled(self):
        raise asyncio.CancelledError("raised by the job")

    @bridge
    async def via_nested(self):
        return await self.raise_cancelled()


@pytest.mark.parametrize("method", ["raise_cancelled", "via_nested"])
def test_cancelled_error_raised_by_a_job_reaches_its_caller_as_an_error(method):
    async def main():
        worker = RaisesCancelled()
        with pytest.raises(asyncio.CancelledError, match="raised by the job"):
            await getattr(worker, method)()
        assert asyncio.current_task().cancelling() == 0  # Raised, not a cancellation of this task
        outcome = await in_thread(lambda: getattr(worker, method)().result(2))
        assert type(outcome["error"]) is asyncio.CancelledError and outcome["error"].args == ("raised by the job",)
        await stop(worker)

    asyncio.run(main())


def test_cancelling_a_call_cancels_its_job_quietly(caplog):
    async def main():
        worker = Tidy()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(worker.tidy(), 0.1)
        await until(lambda: getattr(worker, "tidied", False))  # The job got the cancellation, and cleaned up
        await until(lambda: not worker._calls)
        await stop(worker)
        gc.collect()

    asyncio.run(main())
    gc.collect()
    assert "never retrieved" not in caplog.text  # Like a task cancelled and never awaited


class FailsCleanup(Worker):
    @bridge
    async def failing_cleanup(self):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise ValueError("cleanup failed")


def test_cancelled_job_failing_in_cleanup_is_logged_as_in_asyncio(caplog):
    async def main():
        worker = FailsCleanup()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(worker.failing_cleanup(), 0.1)
        await asyncio.sleep(0.1)
        await stop(worker)
        gc.collect()

    asyncio.run(main())
    gc.collect()
    assert "exception was never retrieved" in caplog.text and "cleanup failed" in caplog.text


# Teardown


class LeavesWork(Worker):
    def __init__(self):
        self.left = []
        super().__init__()

    @bridge
    async def leave(self, kind):
        async def polite():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                self.left.append("polite cancelled")
                raise

        async def stubborn():
            while True:
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.sleep(10)

        async def ticks():
            while True:
                yield
                await asyncio.sleep(0.01)

        async def consumer():
            try:
                async for _ in ticks():
                    pass
            finally:
                self.left.append("consumer cleaned up")

        loop = asyncio.get_running_loop()
        match kind:
            case "polite":
                loop.create_task(polite())
            case "stubborn":
                loop.create_task(stubborn())
            case "generator":
                loop.create_task(consumer())
            case "executor":
                loop.create_task(asyncio.to_thread(time.sleep, 1))
            case "suspended":

                async def generator():
                    try:
                        yield
                    finally:
                        self.left.append("generator closed")

                self.suspended = generator()
                await anext(self.suspended)  # Kept, so that only the loop's shutdown_asyncgens() closes it
        await asyncio.sleep(0.05)


@pytest.mark.parametrize(
    "kind, left",
    [
        ("polite", ["polite cancelled"]),
        ("generator", ["consumer cleaned up"]),
        ("suspended", ["generator closed"]),
        ("stubborn", []),
        ("executor", []),
    ],
)
def test_work_left_running_is_cancelled_then_abandoned(kind, left, caplog):
    async def main():
        worker = LeavesWork()
        await worker.leave(kind)
        started = time.monotonic()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)  # The executor's, about threads it abandoned
            await stop(worker)
        assert time.monotonic() - started < 0.5
        assert worker.left == left

    asyncio.run(main())
    gc.collect()
    assert ("Task was destroyed but it is pending" in caplog.text) == (kind == "stubborn")


class Late(Worker):
    late = None

    def _finalize(self):
        # After the loop has run for the last time, but before the worker fails what's pending: submitting as a call
        # made just before the stop, which the loop then never ran, would be. (Past bridge()'s refusing calls once the
        # worker has stopped.) Only on the worker's own thread, as it finishes: not in a forked process, whose copy's loop
        # has closed.
        if self.in_worker:
            with self._submitting:
                Late.late = AwaitableFuture(self._loop)
                self._pending.add(Late.late)
                self._loop.call_soon_threadsafe(self._start, Late.late, Worker.work.func, (), {})
        super()._finalize()


def test_submission_the_loop_never_runs_fails_instead_of_hanging():
    async def main():
        await stop(Late())
        with pytest.raises(CleanStop):
            Late.late.result(1)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        asyncio.run(main())
        gc.collect()
    assert not [warning for warning in caught if "never awaited" in str(warning.message)]


# Processes


def fork() -> int:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)  # About forking a process with threads
        return os.fork()


def child_status(pid: int, timeout: float = 10) -> int:
    """The child's exit status, killing it if it hasn't exited by `timeout`: a hang in the child fails the test, rather
    than hangs it, even one in the worker's fork handler, which the child runs before os.fork() returns there."""
    deadline = time.monotonic() + timeout
    while (waited := os.waitpid(pid, os.WNOHANG))[0] == 0:
        if time.monotonic() > deadline:
            os.kill(pid, signal.SIGKILL)
            return os.waitstatus_to_exitcode(os.waitpid(pid, 0)[1])
        time.sleep(0.01)
    return os.waitstatus_to_exitcode(waited[1])


class Remembers(Worker):
    @bridge
    async def waits(self):
        try:
            await asyncio.sleep(10)
        finally:
            self.cleaned_up_in = os.getpid()


class Forks(Worker):
    def __init__(self, other):
        self.other, self.hooked = other, False
        super().__init__()

    def on_stop_requested(self):
        self.hooked = True

    def on_shutdown(self):
        self.hooked = True

    @bridge
    async def forks(self):
        if (pid := fork()) == 0:  # The child carries on in this call, on its copy of the worker's loop
            try:
                await self.work()  # Made and run on the worker's thread, as its forking thread is that here
            except WorkerStopped as refused:
                os._exit(
                    0
                    if "forked" in refused.args[0]
                    and self._stopped.done()
                    and self.other._stopped.done()
                    and all(future.done() for future in self._pending)  # This call's own, for one
                    and not self.hooked  # Not on the worker's thread here, though this one was it
                    else 1
                )
            finally:
                os._exit(2)  # Pytest's machinery mustn't carry on in the child
        return await asyncio.to_thread(child_status, pid)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork()")
def test_a_forked_process_cant_use_its_copies_of_workers_nor_wait_for_them():
    worker, other = Remembers(), Worker()
    in_flight = worker.waits()  # Submitted as made, so in flight as the process forks
    holding, release = threading.Event(), threading.Event()

    def hold():
        with worker._submitting:  # Held as the process forks
            holding.set()
            release.wait()

    holder = threading.Thread(target=hold)
    holder.start()
    holding.wait()
    if (pid := fork()) == 0:
        try:
            refused = worker.work().exception(1)  # Needs the lock, which a thread that isn't here holds
            ok = (
                type(refused) is WorkerStopped
                and "forked" in refused.args[0]
                and type(in_flight.exception(1)) is WorkerStopped  # Rather than a TimeoutError: nothing here runs it
                and type(worker.stop(wait=False).exception(1)) is WorkerStopped
                and type(worker.stop().exception(1)) is WorkerStopped  # Rather than waiting for it to finish
                and getattr(worker, "cleaned_up_in", None) != os.getpid()  # Cleanup meant for the parent, not run here
                and type(other.work().exception(1)) is WorkerStopped
            )
            os._exit(0 if ok else 1)
        finally:
            os._exit(2)
    release.set()
    holder.join()
    assert child_status(pid) == 0
    assert worker.work().result(1) == "done" and not worker._stopped.done()  # The parent's carries on
    asyncio.run(stop(worker, interrupt=True))
    asyncio.run(stop(other))
    assert isinstance(in_flight.exception(1), concurrent.futures.CancelledError)  # Interrupted, here
    assert worker.cleaned_up_in == os.getpid()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork()")
def test_a_call_that_forks_carries_on_only_in_the_parent():
    worker = Forks(other := Worker())
    assert worker.forks().result(5) == 0
    assert worker.work().result(1) == "done" and not worker._stopped.done()
    asyncio.run(stop(worker))
    asyncio.run(stop(other))



@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork()")
def test_workers_the_fork_handler_misses_are_stopped_in_the_child_once_used():
    worker = Worker()
    CaptiveWorker._WORKERS.discard(worker)  # As one would be if registered just after the fork, say
    if (pid := fork()) == 0:
        try:
            refused = worker.work().exception(1)
            ok = type(refused) is WorkerStopped and "forked" in refused.args[0]
            os._exit(0 if ok and type(worker.stop().exception(1)) is WorkerStopped else 1)
        finally:
            os._exit(2)
    assert child_status(pid) == 0
    if (pid := fork()) == 0:  # stop() first, which waits on _finished, rather than on a call
        try:
            os._exit(0 if type(worker.stop().exception(1)) is WorkerStopped else 1)
        finally:
            os._exit(2)
    assert child_status(pid) == 0
    asyncio.run(stop(worker))



@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork()")
def test_hooks_arent_called_in_a_forked_process():
    worker = Hooked()
    if (pid := fork()) == 0:
        try:
            worker.stop().exception(1)  # Stopped here, by the fork handler, and finished
            os._exit(0 if worker._finished.done() and worker.hooks == [] else 1)
        finally:
            os._exit(2)
    assert child_status(pid) == 0
    asyncio.run(stop(worker))
    worker._thread.join(1)
    assert [hook for hook, *_ in worker.hooks] == ["stop requested", "shutdown"]
