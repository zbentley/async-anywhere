"""EventLoopExecutors with async libraries other than asyncio: callers on trio, or using anyio's API on any of its
backends, and parent tasks using anyio, or trio, run as a guest on the worker's loop (see trio_guest()). The worker's own
loop is asyncio's, or uvloop's: see executor_loop()."""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars

import anyio
import pytest
import trio

from async_anywhere.exceptions import CleanStop, EventLoopExecutorStopped
from async_anywhere.futures import AwaitableFuture
from tests.test_executors.utils import Tidy, bridge, running


pytestmark = pytest.mark.backends("asyncio", "uvloop", "trio")


current = contextvars.ContextVar("current", default="unset")


async def sees():
    await anyio.lowlevel.checkpoint()  # anyio works in jobs, as on any asyncio loop
    return anyio.get_cancelled_exc_class(), current.get()


class Runtimes(Tidy):
    @bridge
    async def fails(self, error):
        raise error

    @bridge
    async def sees(self):
        return await sees()


# Callers


async def test_calls_can_be_awaited_with_any_library():
    async with running(Runtimes) as worker:
        assert await worker.work(0.01, "awaited") == "awaited"
        with pytest.raises(ValueError) as raised:
            await worker.fails(error := ValueError())
        assert raised.value is error
        results = []

        async def wait(call):
            results.append(await call)

        async with anyio.create_task_group() as group:  # Made together, so run together
            group.start_soon(wait, worker.waits_for(release := AwaitableFuture()))
            group.start_soon(wait, worker.waits_for(release))
            await worker.reached("waits_for", 2)
            release.set_result("released")
        assert results == ["released", "released"]


async def test_jobs_run_on_asyncio_in_a_copy_of_their_callers_context_whatever_the_caller_runs_on():
    # anyio.run() (as its pytest plugin does) records its backend in a context variable of sniffio's, which jobs mustn't
    # inherit: the job would take itself for being on trio
    current.set("caller's")
    async with running(Runtimes) as worker:
        assert await worker.sees() == (asyncio.CancelledError, "caller's")


async def test_cancelling_a_caller_cancels_its_call_once():
    async with running(Runtimes) as worker:
        worker.mark("release")  # Its cleanup need only happen
        call = worker.tidy()

        async def wait():
            await call

        async with anyio.create_task_group() as group:
            group.start_soon(wait)
            await worker.reached("tidy")
            group.cancel_scope.cancel()
        assert await worker.reached("tidied") == 1  # The job got the cancellation once, and cleaned up
        assert call.cancelled()


async def test_interrupted_calls_raise_why_rather_than_cancelling_their_callers():
    async with running(Runtimes) as worker:
        call, raised = worker.work(10), AwaitableFuture()

        async def wait():
            try:
                await call
            except concurrent.futures.CancelledError as error:  # Not asyncio's, on asyncio: see below
                raised.set_result(error)

        async with anyio.create_task_group() as group:
            group.start_soon(wait)
            await worker.reached("work")
            await worker.shutdown(wait=False, cancel_futures=True)
        error = await raised
        assert isinstance(error, concurrent.futures.CancelledError)
        assert not isinstance(error, anyio.get_cancelled_exc_class())  # Not the caller's own cancellation
        assert type(error.__cause__) is EventLoopExecutorStopped


async def test_giving_up_waiting_on_stop_cancels_nothing_with_any_library():
    async with running(Runtimes) as worker:
        call = worker.waits_for(release := AwaitableFuture())
        await worker.reached("waits_for")
        with anyio.move_on_after(0.05):
            await worker.shutdown(wait=False)  # A drain, waiting for the call
        assert worker.state is worker.Stopping
        release.set_result("finished")
        assert await call == "finished"
        assert type(await worker.shutdown(wait=False)) is CleanStop


@pytest.mark.backends("trio")
async def test_trio_callers_use_trio_as_usual():
    async with running(Runtimes) as worker:
        worker.mark("release")  # Its cleanup need only happen
        results = []

        async def wait(call):
            results.append(await call)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(wait, worker.work(0.01, "first"))
            nursery.start_soon(wait, worker.work(0, "second"))
        assert sorted(results) == ["first", "second"]
        with trio.move_on_after(0.05) as scope:
            await worker.tidy()
        assert scope.cancelled_caught and await worker.reached("tidied") == 1


# Parent tasks


class AnyioRun(Runtimes):
    async def _run(self):
        async with anyio.create_task_group() as group:
            group.start_soon(self.child)
            await self.runloop

    async def child(self):
        self.mark("child")
        try:
            await anyio.sleep_forever()
        finally:
            with anyio.CancelScope(shield=True):  # anyio's task groups cut cleanup that isn't shielded short
                await anyio.sleep(0.01)
            self.mark("child cleaned up")


async def test_parent_tasks_can_use_anyio():
    async with running(AnyioRun) as worker:
        await worker.reached("child")
        assert await worker.sees() == (asyncio.CancelledError, "unset")
        stopped = await worker.shutdown(wait=False, cancel_futures=True)
        assert stopped.args == ("(idle) manually stopped",)
        assert worker.reached("child cleaned up").done()


def trio_guest(trio_main):
    """A parent task running `await trio_main(worker)` in trio, as a guest on the worker's loop. It ends as trio_main()
    does, and interrupting it cancels trio_main(), and waits for the run to finish."""

    async def parent_task(worker):
        loop = asyncio.get_running_loop()
        done, token, scope = loop.create_future(), loop.create_future(), trio.CancelScope()

        async def main():
            token.set_result(trio.lowlevel.current_trio_token())
            with scope:
                return await trio_main(worker)

        trio.lowlevel.start_guest_run(
            main,
            run_sync_soon_threadsafe=loop.call_soon_threadsafe,
            run_sync_soon_not_threadsafe=loop.call_soon,
            done_callback=done.set_result,
        )
        while not done.done():
            try:
                await asyncio.wait([done])  # Which, unlike awaiting it, cancelling this task doesn't cancel
            except asyncio.CancelledError:
                # Through the run, so it wakes even if it's waiting for I/O; unless main() has yet to start
                token.result().run_sync_soon(scope.cancel) if token.done() else scope.cancel()
        result = done.result().unwrap()
        if scope.cancelled_caught:
            raise asyncio.CancelledError  # Cancelled, as an asyncio task, as the worker cancelled it
        return result

    return parent_task


async def test_parent_tasks_can_run_trio_as_a_guest():
    marks = {name: AwaitableFuture() for name in ("started", "cleaned up")}

    async def trio_main(worker):
        bridged = worker.bridge(sees)  # Bridged here, and run, on asyncio, as ever
        # Awaited in trio, on the worker's own thread, which is in no job
        marks["started"].set_result((await bridged(), worker.current_job()))
        try:
            await trio.sleep_forever()
        finally:
            with trio.CancelScope(shield=True):
                await trio.sleep(0.01)  # Cleanup that awaits, in trio
            marks["cleaned up"].set_result(trio.lowlevel.in_trio_task())

    async with running(main=trio_guest(trio_main)) as worker:
        assert await marks["started"] == ((asyncio.CancelledError, "unset"), None)
        stopped = await worker.shutdown(wait=False, cancel_futures=True)
        assert stopped.args == ("(idle) manually stopped",)
        assert marks["cleaned up"].result_or_none() is True


@pytest.mark.parametrize(
    "end, kind, text",
    [
        (lambda: None, CleanStop, "(idle) main task returned"),
        (lambda: 1 / 0, EventLoopExecutorStopped, "(idle) main task raised ZeroDivisionError('division by zero')"),
    ],
    ids=["returns", "raises"],
)
async def test_a_trio_guest_ending_stops_the_executor(end, kind, text):
    async def trio_main(worker):
        await trio.sleep(0.01)
        end()

    async with running(main=trio_guest(trio_main)) as worker:
        stopped = await worker.wait_for_state(worker.Shutdown)  # Of its own accord
        assert type(stopped) is kind and stopped.args == (text,)
