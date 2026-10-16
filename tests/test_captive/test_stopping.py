"""How CaptiveWorkers stop: idle expiry, the parent task ending, interrupts, crashes, drains, stop(), and the teardown."""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import gc
import sys
import time
import traceback

import pytest

from async_anywhere.captive import CaptiveWorker
from async_anywhere.captive._exceptions import CleanStop, WorkerBroken, WorkerStopped
from async_anywhere.captive._stopped import WorkerStopFuture
from async_anywhere.futures import AwaitableFuture
from tests.test_captive.utils import (
    TIMING_SENSITIVE,
    BlocksLoop,
    SlowStop,
    Tidy,
    Worker,
    bridge,
    reason,
    running,
    settled,
)


# Expiry


IDLE = 0.05  # The idle timeout of the workers that expire


class Expiring(Worker):
    idle_timeout = IDLE


class SlowCleanup(Expiring):
    async def _run(self):
        try:
            await self.runloop
        except asyncio.CancelledError:
            await asyncio.sleep(IDLE * 2.5)  # Longer than the idle timeout. Cancelling this again would cut it short.
            self.mark("cleaned up")
            raise


async def test_expires_when_idle():
    async with running(Expiring) as worker:
        stopped = await worker._finished  # Of its own accord
        assert type(stopped) is CleanStop and stopped.args == ("(idle) expired",)


@TIMING_SENSITIVE
async def test_countdown_restarts_after_the_last_call():
    async with running(Expiring) as worker:
        await asyncio.sleep(IDLE / 2)
        await worker.work(IDLE)  # Outlasting the countdown as it stood when the call was made
        started = time.monotonic()
        await worker.reached("stopped")
        assert IDLE * 0.75 < time.monotonic() - started < IDLE * 3


@TIMING_SENSITIVE
async def test_calls_in_flight_hold_off_expiry():
    async with running(Expiring) as worker:
        assert await worker.work(IDLE * 1.5, "alone") == "alone"
        short, long = worker.work(IDLE / 4), worker.work(IDLE * 3, "long")
        assert await short == "done"
        assert await long == "long"  # Not cut short by expiry, though the short call ended long before it
        assert not worker.reached("stopped").done()
        await worker.reached("stopped")
        assert reason(worker) == "(idle) expired"


class BlockedAtFirst(Expiring):
    async def _run(self):
        time.sleep(IDLE * 2)  # Blocking the worker's loop, synchronously, past the idle timeout
        await self.runloop


@TIMING_SENSITIVE
async def test_calls_made_before_expiry_hold_it_off_though_the_loop_is_blocked():
    async with running(BlockedAtFirst) as worker:
        await asyncio.sleep(IDLE / 2)
        assert await worker.work() == "done"  # Taken by the loop before the overdue countdown fires
        await worker.reached("stopped")
        assert reason(worker) == "(idle) expired"


async def test_nested_call_holds_off_expiry_until_the_outer_one_ends():
    async with running(Expiring) as worker:
        assert await worker.nested(IDLE * 3) == "outer done"
        assert not worker.reached("stopped").done()


class CallsFromRun(Expiring):
    async def _run(self):
        self.mark("result", await self.work(IDLE * 3, "from _run()"))
        await self.runloop


async def test_call_from_run_holds_off_expiry():
    async with running(CallsFromRun) as worker:
        assert await worker.reached("result") == "from _run()" and not worker.reached("stopped").done()
        await worker.reached("stopped")
        assert reason(worker) == "(idle) expired"


@TIMING_SENSITIVE
async def test_calls_during_expiry_cleanup_are_refused():
    async with running(SlowCleanup) as worker:
        await worker.reached("stopped")
        with pytest.raises(CleanStop, match=r"\(idle\) expired"):
            await worker.work()
        assert not worker.reached("cleaned up").done()  # Refused while _run() was still cleaning up
        await worker.stop()
        assert worker.reached("cleaned up").done()


# The parent task ending


class StopsItself(Worker):
    async def _run(self):
        await self.runloop
        await self.stop(interrupt=True, wait=False)


@pytest.mark.parametrize(
    "cls, end, kind, text",
    [
        (Worker, lambda runloop: runloop.set_result(None), CleanStop, "(interrupted) parent task returned"),
        (
            Worker,
            lambda runloop: runloop.set_exception(ValueError("boom")),
            WorkerStopped,
            "(interrupted) parent task raised ValueError('boom')",
        ),
        (StopsItself, lambda runloop: runloop.set_result(None), WorkerStopped, "(interrupted) manually stopped"),
    ],
    ids=["returns", "raises", "stops itself"],
)
async def test_run_ending_stops_the_worker(cls, end, kind, text):
    async with running(cls) as worker:
        call = worker.work(10)
        await worker.reached("work")
        end(worker.runloop)
        with pytest.raises(concurrent.futures.CancelledError):
            await call  # In flight as _run() ends, so interrupted
        stopped = await worker._finished  # Of its own accord
        assert type(stopped) is kind and stopped.args == (text,)


# Interrupts


async def test_interrupt_cancels_calls_once_and_tells_callers_why():
    async with running(Tidy) as worker:
        call = worker.tidy()
        await worker.reached("tidy")
        stopping = worker.stop(interrupt=True)
        await worker.reached("_run ended")
        await settled(worker)  # Past whatever the worker does as the parent task ends, before it waits for the call
        worker.mark("release")
        stopped = await stopping
        assert type(stopped) is WorkerStopped and stopped.args == ("(interrupted) manually stopped",)
        assert worker.reached("tidied").result_or_none() == 1  # Cancelled once: its cleanup wasn't cut short
        with pytest.raises(concurrent.futures.CancelledError) as raised:
            await call
        error = raised.value
        assert not isinstance(error, asyncio.CancelledError)  # Which asyncio would take for this task being cancelled
        assert "tidy" in [frame.name for frame in traceback.extract_tb(error.__traceback__)]  # Where it was cancelled
        why = error.__cause__
        assert type(why) is WorkerStopped and why.args == stopped.args and why is not stopped  # A copy
        assert why.__cause__ is None and stopped.__cause__ is None


async def test_interrupted_callers_see_what_stopped_the_worker():
    async with running(Worker) as worker:
        call = worker.work(10)
        await worker.reached("work")
        worker.runloop.set_exception(ValueError("boom"))
        with pytest.raises(concurrent.futures.CancelledError) as raised:
            await asyncio.to_thread(call.result, 5)
        why = raised.value.__cause__
        assert type(why) is WorkerStopped and why.args == (reason(worker),)
        assert type(why.__cause__) is ValueError and why.__cause__.args == ("boom",)


class RunAwaitsACall(Worker):
    async def _run(self):
        try:
            await self.work(10)
        except BaseException as error:
            self.mark("raised", error)
            raise


async def test_interrupted_calls_on_the_workers_thread_are_cancelled_as_usual():
    async with running(RunAwaitsACall) as worker:
        await worker.reached("work")
        await worker.stop(interrupt=True)
        assert type(await worker.reached("raised")) is asyncio.CancelledError  # Not converted, as for other threads


class Supervises(Tidy):
    async def _run(self):
        async with asyncio.TaskGroup() as group:
            for _ in range(3):
                group.create_task(self.child())
            await self.runloop

    async def child(self):
        await self.tidy()  # So its cancellation, by _run()'s TaskGroup as it's interrupted, cancels the call too


class CancelsWhatItLeaves(Supervises):
    async def _run(self):
        # So cancelling this task cancels the call too, unless the worker already has
        left = asyncio.create_task(self.child())
        try:
            await self.runloop
        finally:
            left.cancel()
            await asyncio.sleep(0)  # Into the call's cleanup, which _run() doesn't wait for


@pytest.mark.parametrize("cls, calls", [(Supervises, 3), (CancelsWhatItLeaves, 1)], ids=["task group", "run cancels"])
def test_an_interrupt_cancels_calls_once_though_what_awaits_them_is_cancelled_too(cls, calls, scale):
    # Cancelled by the interrupt and again through what awaits them, calls cleaning up would be cut short
    with contextlib.ExitStack() as stack:
        workers = [stack.enter_context(running(cls)) for _ in range(10 * scale)]
        for worker in workers:
            worker.reached("tidy", calls).result(2)
        stops = [worker.stop(interrupt=True) for worker in workers]
        for worker in workers:
            # _run() ends only once what awaits the calls has been cancelled, passing that on to the calls' futures
            worker.reached("_run ended").result(2)
            settled(worker).result(2)  # Which then pass it on to the calls' jobs
            worker.mark("release")
        assert all(isinstance(stop.result(2), WorkerStopped) for stop in stops)
    tidied = [worker.reached("tidied", n).result_or_none() for worker in workers for n in range(1, calls + 1)]
    assert tidied == [1] * calls * 10 * scale


class LeavesACall(Tidy):
    async def _run(self):
        self.left = self.tidy()  # Not awaited, so nothing but the worker cancels it
        await self.runloop


async def test_calls_on_the_workers_thread_are_interrupted_like_any_other():
    async with running(LeavesACall) as worker:
        worker.mark("release")  # Its cleanup need only happen
        await worker.reached("tidy")
        await worker.stop(interrupt=True)
        assert worker.reached("tidied").result_or_none() == 1
        with pytest.raises(concurrent.futures.CancelledError, check=lambda error: isinstance(error.__cause__, WorkerStopped)):
            worker.left.result(1)  # Interrupted, and told why


class Exits(Worker):
    @bridge
    async def exit(self):
        sys.exit(3)

    @bridge
    async def slow(self):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(0.01)  # Within the teardown's 0.05s
            self.cleaned_up = True
            raise


@pytest.mark.logs_asyncio_errors  # As the crashing job's exception is never retrieved
async def test_a_crash_stops_the_worker_and_interrupts_calls_in_flight(caplog, thread_exceptions):
    async with running(Exits) as worker:
        slow = worker.slow()
        await worker.reached("slow")
        with pytest.raises(SystemExit) as exiting:
            worker.exit().result(2)  # Raised again in its caller, as awaiting it in-loop would
        with pytest.raises(concurrent.futures.CancelledError) as raised:
            await slow
        why = raised.value.__cause__
        assert why.args == ("(interrupted) the worker's event loop crashed",) and type(why.__cause__) is SystemExit
        assert worker.cleaned_up  # Cancelled once, by the teardown
        await asyncio.to_thread(worker._thread.join, 1)
        frames = [frame.name for frame in traceback.extract_tb(exiting.value.__traceback__)]
        assert "exit" in frames and "_main" not in frames  # A copy, untouched by the worker raising the original
        assert (await worker.stop()).args == why.args
    assert [type(error) for error in thread_exceptions()] == [SystemExit]  # Raised again on the worker's thread
    assert caplog.text.count("Stopping due to error") == 1


class RunCrashes(Exits):
    async def _run(self):
        await self.runloop
        raise KeyboardInterrupt


@pytest.mark.logs_asyncio_errors  # Likewise, for the parent task's
async def test_a_crash_in_run_stops_the_worker_and_interrupts_calls_in_flight(thread_exceptions):
    async with running(RunCrashes) as worker:
        call = worker.slow()
        await worker.reached("slow")
        worker.runloop.set_result(None)
        with pytest.raises(concurrent.futures.CancelledError) as raised:
            await call
        why = raised.value.__cause__
        assert why.args == ("(interrupted) the worker's event loop crashed",)
        assert type(why.__cause__) is KeyboardInterrupt
        assert worker.cleaned_up  # Cancelled once, by the teardown
    assert [type(error) for error in thread_exceptions()] == [KeyboardInterrupt]  # Raised again on its thread


@TIMING_SENSITIVE
async def test_interrupting_during_expiry_cleanup_cancels_run_again():
    async with running(SlowCleanup) as worker:
        await worker.reached("stopped")
        stopped = await worker.stop(interrupt=True)
        assert stopped.args == ("(idle) expired",) and worker._stopped.interrupted
        assert not worker.reached("cleaned up").done()  # Cut short


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
                self.mark(f"{who} cancelled")
                if self.cancels[who] == 2:
                    raise


async def test_each_interrupting_stop_cancels_run_and_calls_again():
    async with running(Endures) as worker:
        call = worker.call()
        await worker.reached("call")
        await worker.stop(interrupt=True, wait=False)
        await worker.reached("_run cancelled")
        await worker.reached("call cancelled")
        await worker.stop(wait=False)  # Doesn't interrupt, so cancels nothing
        await settled(worker)  # By when any cancellation it sent has landed
        assert worker.cancels == {"_run": 1, "call": 1}
        await worker.stop(interrupt=True)
        assert worker.cancels == {"_run": 2, "call": 2}
        with pytest.raises(concurrent.futures.CancelledError):
            await call


async def test_a_callers_cancellation_after_an_interrupt_doesnt_cancel_the_call_again():
    async with running(Tidy) as worker:
        call = worker.tidy()
        await worker.reached("tidy")
        await worker.stop(interrupt=True, wait=False)
        assert call.cancel()  # While it's cleaning up
        await settled(worker)  # By when the cancellation has reached the job, if it was going to
        worker.mark("release")
        await worker.stop()
        assert worker.reached("tidied").result_or_none() == 1  # Not cut short


@pytest.mark.parametrize("interrupt", [False, True])
async def test_stop_cleanup_outlasting_the_idle_timeout_isnt_cut_short(interrupt):
    async with running(SlowCleanup) as worker:
        await worker.stop(interrupt=interrupt)
        assert worker.reached("cleaned up").done()


class WithHelper(Tidy):
    async def _run(self):
        async with asyncio.TaskGroup() as group:
            group.create_task(self.tidies())


@pytest.mark.parametrize("interrupt", [False, True])
async def test_runs_own_tasks_are_left_to_it(interrupt):
    async with running(WithHelper) as worker:
        await worker.reached("tidies")
        stopping = worker.stop(interrupt=interrupt)
        await settled(worker)  # By when the stop has cancelled whatever it would
        worker.mark("release")
        await stopping
        # Cancelled once, by _run()'s task group, and not again by the worker
        assert worker.reached("tidied").result_or_none() == 1


class Retries(Worker):
    @bridge
    async def retry_forever(self):
        while True:
            try:
                async with asyncio.timeout(0.1):
                    try:
                        await asyncio.sleep(10)
                    except asyncio.CancelledError:
                        self.mark("timing out")
                        # Still inside the timeout, which is expiring, as the interrupt lands
                        await self.reached("release")
                        raise
            except TimeoutError:
                pass


async def test_interrupt_isnt_lost_to_a_timeout_inside_the_call():
    async with running(Retries) as worker:
        call = worker.retry_forever()
        await worker.reached("timing out")
        await worker.stop(interrupt=True, wait=False)  # Once it has begun, so has cancelled the call
        worker.mark("release")
        await asyncio.wait_for(worker.reached("shutdown"), 3)
        with pytest.raises(concurrent.futures.CancelledError):
            await call


# Drains


async def test_drain_refuses_new_calls_and_lets_calls_in_flight_finish():
    async with running(Worker) as worker:
        call = worker.waits_for(release := AwaitableFuture())
        await worker.reached("waits_for")
        stopped = await asyncio.wait_for(worker.stop(wait=False), 1)  # Only until the stop began
        assert type(stopped) is CleanStop and stopped.args == ("(waiting for running jobs) manually stopped",)
        with pytest.raises(CleanStop):
            await worker.work()
        release.set_result("finished")
        assert await call == "finished"
        await worker.stop()
        assert worker._loop.is_closed()


class CallsThenStops(Worker):
    async def _run(self):
        self.call = self.work(0.01, "finished")
        await self.stop(wait=False)


async def test_calls_made_just_before_a_drain_finish(scale):
    for _ in range(10 * scale):
        async with running(Worker) as worker:
            call = worker.work(0.01, "finished")
            stopped = await worker.stop()  # Queued behind the call
            assert stopped.args == ("(waiting for running jobs) manually stopped",)
            assert await call == "finished"
    async with running(CallsThenStops) as worker:  # Likewise on the worker's own thread
        await worker.reached("stopped")
        assert reason(worker) == "(waiting for running jobs) manually stopped"
        assert await worker.call == "finished"


async def test_calls_made_during_a_drain_are_refused_without_waiting_for_the_loop():
    async with running(BlocksLoop) as worker:
        blocking = worker.blocks(go := AwaitableFuture())
        await worker.reached("blocks")
        assert "waiting for running jobs" in (await worker.stop(wait=False)).args[0]
        go.set_result(None)
        await worker.reached("blocked")  # Its loop is blocked now
        refused = worker.work()
        assert refused.done() and type(refused.exception()) is CleanStop
        worker.mark("release")
        await blocking


async def test_calls_made_after_a_stop_is_queued_are_refused_once_it_begins():
    async with running(BlocksLoop) as worker:
        blocking = worker.blocks()
        await worker.reached("blocked")  # So the stop can't begin yet
        worker.stop(wait=False)
        call = worker.work()
        assert not call.done()  # Not refused yet, as the worker hasn't stopped
        worker.mark("release")
        with pytest.raises(CleanStop):
            await call
        await blocking


async def test_abandoned_drain_keeps_going_and_can_be_cut_short():
    async with running(Worker) as worker:
        call = worker.waits_for(release := AwaitableFuture())
        await worker.reached("waits_for")
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(worker.stop(), 0.05)
        release.set_result("finished")
        assert await call == "finished"  # The drain went on regardless
        await worker.stop()

    async with running(Worker) as worker:
        call = worker.work(10)
        await worker.reached("work")
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(worker.stop(), 0.05)
        stopped = await worker.stop(interrupt=True)
        assert stopped.args == ("(waiting for running jobs) manually stopped",)  # The first stop's reason stands
        with pytest.raises(concurrent.futures.CancelledError):
            await call


async def test_draining_caller_gets_why_when_another_stop_finishes_the_worker(scale):
    for _ in range(10 * scale):
        async with running(Worker) as worker:
            call = worker.work(5)
            await worker.reached("work")
            drain = asyncio.ensure_future(worker.stop())
            await worker.stop(interrupt=True)
            assert type(await drain) is CleanStop  # Not asyncio.CancelledError, though the teardown may cancel it
            with pytest.raises(concurrent.futures.CancelledError):
                await call


class ReturnsWhenStopped(Worker):
    async def _run(self):
        await self.reached("stopped")  # Returns as soon as the worker stops, rather than waiting to be cancelled


async def test_a_drain_outlasts_run_returning():
    async with running(ReturnsWhenStopped) as worker:
        call = worker.waits_for(release := AwaitableFuture())
        await worker.reached("waits_for")
        stopped = await worker.stop(wait=False)
        assert stopped.args == ("(waiting for running jobs) manually stopped",)
        await worker.reached("_run ended")
        release.set_result("finished")
        assert await call == "finished"  # Not interrupted, as _run() ended after the stop, not of its own accord


class RunCalls(Worker):
    async def _run(self):
        await self.work(0.1)
        await self.runloop


async def test_drain_ends_when_runs_own_call_does():
    async with running(RunCalls) as worker:
        await worker.reached("work")
        stopped = await asyncio.wait_for(worker.stop(), 2)
        assert stopped.args == ("(waiting for running jobs) manually stopped",)


class Spawner(Expiring):
    @bridge
    async def spawn(self, seconds):
        asyncio.get_running_loop().create_task(asyncio.sleep(seconds))


@TIMING_SENSITIVE
async def test_tasks_calls_leave_running_dont_hold_anything_up():
    async with running(Spawner) as worker:
        await worker.spawn(10)
        started = time.monotonic()
        await worker.reached("stopped")
        assert time.monotonic() - started < IDLE * 3  # Expired without waiting for that task

    async with running(Spawner) as worker:
        await worker.spawn(10)
        started = time.monotonic()
        await worker.stop()  # A drain
        assert time.monotonic() - started < 0.5


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
        stopped = await self.stop(wait=False)  # Once the stop has begun
        await asyncio.sleep(0)  # This call may still finish
        return stopped


async def test_stop_cant_wait_on_the_workers_own_thread():
    async with running(Stops) as worker:
        with pytest.raises(RuntimeError, match="would wait for itself"):
            await worker.stop_waiting()
        assert not worker.reached("stopped").done()


async def test_interrupting_from_inside_a_call_cancels_it_too():
    async with running(Stops) as worker:
        other = worker.work(10)
        await worker.reached("work")
        with pytest.raises(concurrent.futures.CancelledError):
            await worker.stop_interrupting()
        with pytest.raises(concurrent.futures.CancelledError):
            await other
        assert reason(worker) == "(interrupted) manually stopped"


async def test_draining_from_inside_a_call_returns_at_once_and_lets_calls_finish():
    async with running(Stops) as worker:
        other = worker.waits_for(release := AwaitableFuture())
        await worker.reached("waits_for")
        stopped = await worker.drain_from_inside()
        assert type(stopped) is CleanStop and stopped.args == ("(waiting for running jobs) manually stopped",)
        release.set_result("finished")  # Only now, so it was in flight throughout the drain
        assert await other == "finished"


async def test_stop_can_be_waited_on_from_a_plain_thread():
    async with running(Worker) as worker:
        stopped = await asyncio.to_thread(lambda: worker.stop().result(2))
        assert type(stopped) is CleanStop and stopped.args == ("(idle) manually stopped",)


async def test_giving_up_waiting_on_stop_cancels_nothing():
    async with running(BlocksLoop) as worker:
        call = worker.blocks()
        await worker.reached("blocked")  # So nothing on its loop can finish yet
        for future in worker.stop(wait=False), worker.stop(), call:
            with pytest.raises(TimeoutError):
                future.result(0.05)
            assert not future.cancelled()
        worker.mark("release")
        assert await call is None
        assert (await worker.stop()).args == ("(idle) manually stopped",)  # The stop went ahead, once the call had ended


@TIMING_SENSITIVE
async def test_waiting_for_stop_doesnt_block_the_callers_loop():
    ticks = 0

    async def tick():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.005)

    ticker = asyncio.ensure_future(tick())
    async with running(SlowStop) as worker:
        await worker.stop()
    ticker.cancel()
    assert ticks > 10  # About 30, if never blocked


async def test_stop_future_can_be_waited_on_anywhere_and_not_cancelled():
    # The teardown fails the test if asyncio logs about shielded futures, or what was never retrieved
    async with running(Worker) as worker:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(worker._stopped, 0.05)  # Gives up, which mustn't cancel the stop
        assert not worker._stopped.cancelled()

        async def wait():
            return await worker._stopped

        blocked = asyncio.ensure_future(asyncio.to_thread(worker._stopped.result, 2))
        blocked_too = asyncio.ensure_future(asyncio.to_thread(worker._stopped.result, 2))
        awaited = asyncio.ensure_future(asyncio.to_thread(asyncio.run, wait()))  # On another loop
        stopped = await worker.stop()
        whys = [await blocked, await blocked_too, await awaited, stopped]
        assert all(type(why) is CleanStop and why.args == stopped.args for why in whys)
        assert len({id(why) for why in whys}) == 4  # Each a copy


@TIMING_SENSITIVE
@pytest.mark.parametrize(
    "awaitable", [lambda future: future, lambda future: future.as_async_future()], ids=["awaited", "async"]
)
async def test_giving_up_waiting_on_stop_leaves_other_waiters_alone(awaitable):
    async with running(SlowStop) as worker:

        async def wait():
            return await awaitable(worker.stop())

        other = asyncio.ensure_future(wait())
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(wait(), 0.05)
        later = asyncio.ensure_future(wait())  # Waiting on the same loop after one gave up, before the stop finishes
        assert type(await other) is CleanStop and type(await later) is CleanStop


def test_stop_futures_only_fail_once_the_worker_is_broken():
    with running() as worker:
        with pytest.raises(AssertionError, match="Invariant violation"):
            worker._stopped.set_exception(CleanStop("not like this"))
        assert not worker._stopped.done()
    broken = WorkerStopFuture(worker)
    broken.set_exception(why := WorkerBroken("like this"))
    copy = broken.exception()
    assert type(copy) is WorkerBroken and copy.args == why.args and copy is not why


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
        await asyncio.sleep(0)  # So what it left has started


@pytest.mark.parametrize(
    "kind, left",
    [
        ("polite", ["polite cancelled"]),
        ("generator", ["consumer cleaned up"]),
        ("suspended", ["generator closed"]),
        pytest.param("stubborn", [], marks=pytest.mark.logs_asyncio_errors),  # "Task was destroyed but it is pending!"
        pytest.param(
            "executor",
            [],
            # About the threads it abandoned
            marks=pytest.mark.filterwarnings("ignore:The executor did not finishing joining:RuntimeWarning"),
        ),
    ],
)
async def test_work_left_running_is_cancelled_then_abandoned(kind, left, caplog):
    async with running(LeavesWork) as worker:
        await worker.leave(kind)
        started = time.monotonic()
        await worker.stop()
        assert time.monotonic() - started < 0.5
        assert worker.left == left
    gc.collect()
    assert ("Task was destroyed but it is pending" in caplog.text) == (kind == "stubborn")


def test_submission_the_loop_never_runs_fails_instead_of_hanging(monkeypatch):
    late, finalize = [], CaptiveWorker._finalize

    def finalize_late(worker):
        # After the loop has run for the last time, but before the worker fails what's pending: submitting as a call
        # made just before the stop, which the loop then never ran, would be. (Past bridge()'s refusing calls once the
        # worker has stopped.) Only on the worker's own thread, as it finishes: not in a forked process, whose copy's loop
        # has closed.
        if worker.in_worker():
            with worker._submitting:
                late.append(future := AwaitableFuture(prevent_blocking_wait_in_loops=(worker._loop,)))
                worker._pending.add(future)
                worker._loop.call_soon_threadsafe(worker._start, future, asyncio.sleep, (0,), {})
        finalize(worker)

    monkeypatch.setattr(CaptiveWorker, "_finalize", finalize_late)
    with running() as worker:
        worker.stop().result(2)
    with pytest.raises(CleanStop):
        late[0].result(1)
