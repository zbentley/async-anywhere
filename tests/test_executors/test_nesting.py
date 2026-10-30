"""EventLoopExecutors made on other workers' threads, by their parent tasks or calls: used there, as a worker's client,
or handed out to callers, who can use them from anywhere, whether or not the worker that made them is still running."""
from __future__ import annotations

import asyncio
import concurrent.futures

import anyio
import pytest

from async_anywhere.executors.exceptions import CleanStop, WorkerStopped
from tests.test_executors.utils import Tidy, Worker, bridge, running


pytestmark = pytest.mark.backends("asyncio", "uvloop", "trio")


class Forwards(Worker):
    @bridge
    async def forwards(self, call, *args):
        return await call(*args)  # Made on this worker's thread, from a job there


# Used within the worker that made it


class Owner(Forwards):
    """Makes an inner worker in its parent task, as its client, and stops it, draining it, as that ends."""

    async def _run(self):
        self.inner = Tidy()  # On this worker's thread, briefly blocking its loop as the inner one starts
        self.mark("made", self.inner)
        try:
            assert await self.inner.work(0.01, "from the parent task") == "from the parent task"
            await self.runloop
        finally:
            self.mark("inner stopped", (await self.inner.shutdown(wait=False))[2])


async def test_a_worker_can_use_one_made_in_its_parent_task():
    async with running(Owner) as outer:
        inner = await outer.reached("made")
        assert await outer.forwards(inner.work, 0.01, "via the inner worker") == "via the inner worker"

        @inner.bridge
        async def where():
            return inner.in_worker(), outer.in_worker()

        @inner.bridge
        async def calls_back():
            return await outer.work(0.01, "from the outer worker")  # Made on the inner worker's thread

        assert await outer.forwards(where) == (True, False)  # Run on the inner worker's thread, not the outer's
        assert await outer.forwards(calls_back) == "from the outer worker"


async def test_cancelling_a_call_cancels_the_inner_call_it_awaits():
    async with running(Owner) as outer:
        inner = await outer.reached("made")
        inner.mark("release")  # Its cleanup need only happen
        call = outer.forwards(inner.tidy)
        await inner.reached("tidies")
        assert call.cancel()
        assert await inner.reached("tidied") == 1  # Cancelled once, by its caller: the outer worker's job
        assert await outer.forwards(inner.work) == "done"  # Both carry on


async def test_interrupting_a_worker_cancels_the_inner_calls_its_jobs_await():
    async with running(Owner) as outer:
        inner = await outer.reached("made")
        inner.mark("release")  # Its cleanup need only happen
        call = outer.forwards(inner.tidy)
        await inner.reached("tidies")
        await outer.shutdown(wait=False, cancel_futures=True)
        with pytest.raises(concurrent.futures.CancelledError):
            await call
        assert await inner.reached("tidied") == 1  # Cancelled once, by the outer job, as the inner worker wasn't interrupted
        assert type(await outer.reached("inner stopped")) is CleanStop  # Then drained by the outer parent task's cleanup


@pytest.mark.parametrize("how", ["stopped", "interrupted", "parent task returned"])
async def test_a_worker_can_stop_one_it_owns_as_it_stops(how):
    async with running(Owner) as outer:
        inner = await outer.reached("made")
        if how == "parent task returned":
            outer.runloop.set_result(None)
            await outer.wait_for_state(outer.Stopped)
        else:
            await outer.shutdown(wait=False, cancel_futures=how == "interrupted")
        assert type(await outer.reached("inner stopped")) is CleanStop
        assert inner.state is inner.Stopped
        with pytest.raises(CleanStop):
            await inner.work()


# Handed out to callers


class Maker(Forwards):
    @bridge
    async def make(self):
        return Worker()  # On this worker's thread, in a job


class MakesInRun(Forwards):
    async def _run(self):
        self.inner = Worker()  # Never stopped here: whoever it's handed out to must stop it
        await self.runloop

    @bridge
    async def made(self):
        return self.inner


@pytest.mark.parametrize("cls, method", [(Maker, "make"), (MakesInRun, "made")], ids=["in a call", "in the parent task"])
@pytest.mark.parametrize("outer_is", ["running", "stopped", "interrupted", "parent task returned"])
async def test_a_worker_handed_out_by_the_one_that_made_it_can_be_used_from_anywhere(cls, method, outer_is):
    async with running(cls) as outer:
        inner = await getattr(outer, method)()
        match outer_is:
            case "running":
                assert await outer.forwards(inner.work, 0.01, "from the outer worker") == "from the outer worker"
            case "parent task returned":
                outer.runloop.set_result(None)
                await outer.wait_for_state(outer.Stopped)
            case _:
                await outer.shutdown(wait=False, cancel_futures=outer_is == "interrupted")
        if outer_is != "running":
            assert outer.state is outer.Stopped and not outer._thread.is_alive()
            with pytest.raises(WorkerStopped):
                await outer.work()

        async def on_a_fresh_loop():
            return await inner.work(0.01, "on a fresh loop")

        assert await inner.work(0.01, "on the test's loop") == "on the test's loop"
        assert await anyio.to_thread.run_sync(lambda: inner.work(0.01, "a thread").result(1)) == "a thread"
        assert await anyio.to_thread.run_sync(asyncio.run, on_a_fresh_loop()) == "on a fresh loop"
        assert await inner.nested(0.01) == "outer done"  # Its calls on its own thread
        assert type((await inner.shutdown(wait=False))[2]) is CleanStop
        # Its states' futures' callbacks run on its own thread
        assert await inner.reached("stopped") == (True, inner.Stopping)
        assert await inner.reached("shutdown") == (True, inner.Stopped, True)
