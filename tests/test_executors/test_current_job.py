"""current_job() from code a job awaits, however it awaits it.

On Python 3.14 and later, asyncio records who awaits what, which current_job() follows to a job; before, it finds a job
from the job's own task alone (see its warning). Where even 3.14 gets it wrong, the test is xfailed, strictly, or what it
does is pinned.
"""
from __future__ import annotations

import asyncio
import sys
from unittest import mock

import anyio
import pytest

from async_anywhere.executors import EventLoopExecutor
from tests.test_executors.utils import AWAITED_BY, running


only_with_awaited_by = pytest.mark.skipif(not AWAITED_BY, reason="Before 3.14, current_job() looks no further than a job")


def looks(worker, after: asyncio.Event | None = None):
    """A coroutine that, once `after` is set (or at once, after a yield), returns the current_job() it sees."""

    async def look():
        if after is None:
            await asyncio.sleep(0)
        else:
            await after.wait()
        return worker.current_job()

    return look()


async def test_current_job_is_found_in_a_jobs_own_task_and_coroutines_it_awaits():
    async with running() as worker:

        async def deeper():
            return await looks(worker)  # Awaited, not made a task of: so still in the job's own task

        @worker.bridge
        async def job():
            return worker.current_job(), await deeper()

        call = job()
        assert await call == (call, call)


async def test_current_job_is_found_from_tasks_a_job_awaits_however_it_does_on_314_alone():
    async with running() as worker:

        class Awaits:  # A custom awaitable, awaiting a task in its place
            def __init__(self, task):
                self.task = task

            def __await__(self):
                return self.task.__await__()

        class Group(asyncio.TaskGroup):
            pass

        async def through_wait(awaited):
            await asyncio.wait([awaited])
            return awaited.result()

        async def beside_a_mock():
            mocked = asyncio.create_task(looks(worker))
            mocked.add_done_callback(mock.Mock())
            return await mocked

        async def in_an_anyio_task_group():  # anyio says what its task groups' tasks are awaited by, on 3.14
            seen = []

            async def child():
                seen.append(await looks(worker))

            async with anyio.create_task_group() as group:
                group.start_soon(child)
            return seen[0]

        @worker.bridge
        async def job():
            async with Group() as group:
                grouped = group.create_task(looks(worker))
            return {
                "subclassed TaskGroup": grouped.result(),
                "shield": await asyncio.shield(asyncio.create_task(looks(worker))),
                "wait": await through_wait(asyncio.create_task(looks(worker))),
                "wait_for": await asyncio.wait_for(asyncio.create_task(looks(worker)), 5),
                "nested gather": (await asyncio.gather(asyncio.gather(looks(worker))))[0][0],
                "gather, through wait": (await through_wait(asyncio.gather(looks(worker))))[0],
                "shield of gather": (await asyncio.shield(asyncio.gather(looks(worker))))[0],
                "custom awaitable": await Awaits(asyncio.create_task(looks(worker))),
                "beside a Mock callback": await beside_a_mock(),
                "anyio task group": await in_an_anyio_task_group(),
            }

        call = job()
        expected = call if AWAITED_BY else None
        assert {how for how, found in (await call).items() if found is not expected} == set()


@pytest.mark.xfail(strict=True, reason="asyncio records nothing awaiting as_completed()'s, even on 3.14: python/cpython#156523")
async def test_current_job_is_found_through_as_completed():
    async with running() as worker:

        @worker.bridge
        async def job():
            for next_done in asyncio.as_completed([asyncio.create_task(looks(worker))]):
                return await next_done

        call = job()
        assert await call is call


@pytest.mark.xfail(strict=True, reason="An eager task's first step runs as it's made: before anything can await it")
async def test_current_job_is_found_in_the_eager_first_step_of_a_task_a_job_awaits():
    async with running() as worker:

        async def now():
            return worker.current_job()

        @worker.bridge
        async def job():
            return await asyncio.get_running_loop().create_task(now(), eager_start=True)

        call = job()
        assert await call is call


@pytest.mark.xfail(
    sys.version_info >= (3, 14),  # Where jobs start eagerly under task factories too, their first steps in create_task()
    strict=True,
    reason="A task factory wrapping coroutines hides a job's own from current_job() in its first step",
)
async def test_current_job_is_found_in_a_jobs_first_step_under_a_task_factory_wrapping_coroutines():
    make_loop = EventLoopExecutor.__init__.__kwdefaults__["loop_factory"]  # asyncio's or uvloop's: see conftest.py

    def wrap(loop, coro, **kwargs):
        async def wrapped():
            return await coro

        return asyncio.Task(wrapped(), loop=loop, **kwargs)

    def loop_factory():
        loop = make_loop()
        loop.set_task_factory(wrap)
        return loop

    async with running(loop_factory=loop_factory) as worker:

        @worker.bridge
        async def job():
            first = worker.current_job()
            await asyncio.sleep(0)
            return first, worker.current_job()

        call = job()
        first, later = await call
        assert later is call  # Once it has yielded: found by its task
        assert first is call


@only_with_awaited_by
async def test_gathers_and_shields_are_awaited_by_the_task_that_made_them():
    # Not by whatever awaits them, as 3.14 records it (python/cpython#162061): the same task, unless one task makes it and
    # another awaits it, or nothing awaits it yet
    async with running() as worker:

        @worker.bridge
        async def job():
            loop, release, looked, background = asyncio.get_running_loop(), asyncio.Event(), {}, set()

            async def reports(label):
                looked[label].set_result(await looks(worker, release))

            async def made_elsewhere(make):
                made = loop.create_future()

                async def maker():  # Run in the background, so not awaited by this job
                    made.set_result(make())

                background.add(asyncio.create_task(maker()))
                return await made

            looked.update((label, loop.create_future()) for label in ("unawaited", "gather made elsewhere", "shield"))
            unawaited = asyncio.gather(reports("unawaited"))
            release.set()
            found = {"unawaited gather": await looked["unawaited"]}  # Awaiting something else as it looks
            await unawaited
            release.clear()
            gathered = await made_elsewhere(lambda: asyncio.gather(reports("gather made elsewhere")))
            release.set()
            await gathered
            found["gather made elsewhere"] = looked["gather made elsewhere"].result()
            release.clear()
            shielded = await made_elsewhere(lambda: asyncio.shield(asyncio.create_task(reports("shield"))))
            release.set()
            await shielded
            found["shield made elsewhere"] = looked["shield"].result()
            return found

        call = job()
        assert await call == {"unawaited gather": call, "gather made elsewhere": None, "shield made elsewhere": None}


@only_with_awaited_by
async def test_a_gather_one_job_makes_and_another_awaits_is_the_makers():
    async with running() as worker:
        shared = {}

        @worker.bridge
        async def makes():
            shared["release"] = release = asyncio.Event()
            shared["gathered"] = asyncio.gather(looks(worker, release))
            shared["done"] = done = asyncio.Event()
            await done.wait()  # Running, but awaiting something else, as it's looked for

        @worker.bridge
        async def awaits():
            shared["release"].set()
            try:
                return (await shared["gathered"])[0]
            finally:
                shared["done"].set()

        maker = makes()
        assert await awaits() is maker
        await maker


@only_with_awaited_by
async def test_orphans_of_a_gather_or_shield_a_job_gave_up_on_are_still_its():
    # 3.14 drops each record as what it waits on finishes, not as the gather() or shield() does
    async with running() as worker:

        @worker.bridge
        async def job():
            loop, release = asyncio.get_running_loop(), asyncio.Event()
            looked = {label: loop.create_future() for label in ("gather", "shield")}

            async def reports(label):
                looked[label].set_result(await looks(worker, release))

            async def fails():
                raise ValueError

            with pytest.raises(ValueError):  # Leaving reports() running, as gather() does
                await asyncio.gather(fails(), reports("gather"))
            with pytest.raises(TimeoutError):  # Leaving reports() running, as shield() does
                await asyncio.wait_for(asyncio.shield(asyncio.create_task(reports("shield"))), 0.01)
            release.set()
            return {label: await future for label, future in looked.items()}  # What they set: not awaiting them

        call = job()
        assert await call == {"gather": call, "shield": call}


@only_with_awaited_by
async def test_a_task_two_jobs_await_is_either_ones():
    async with running() as worker:
        shared = {}

        @worker.bridge
        async def first():
            shared["task"] = asyncio.create_task(looks(worker))
            return await shared["task"]

        @worker.bridge
        async def second():
            return await shared["task"]

        one, two = first(), second()
        assert await one in (one, two)  # Which, asyncio doesn't say: a set's order
        await two


async def test_calls_run_on_loops_with_asyncios_eager_task_factory(executor_loop):
    if executor_loop == "uvloop" and not AWAITED_BY:  # Not in this package's hands
        pytest.skip("On 3.13, uvloop gives the factory eager_start for every task, the executor's own included")
    make_loop = EventLoopExecutor.__init__.__kwdefaults__["loop_factory"]

    def loop_factory():
        loop = make_loop()
        loop.set_task_factory(asyncio.eager_task_factory)
        return loop

    async with running(loop_factory=loop_factory) as worker:

        @worker.bridge
        async def job():
            return worker.current_job(), await asyncio.create_task(looks(worker))

        call = job()
        assert await call == (call, call if AWAITED_BY else None)
