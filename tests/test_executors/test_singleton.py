"""SingletonEventLoopExecutor's states, which follow its worker's, from Stopped back around to Running as it replaces
them, and its shutdown()."""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import os

import pytest

from async_anywhere.exceptions import CleanStop, WorkerForked, WorkerStopped, WouldBlock
from async_anywhere.executors.singleton import SingletonEventLoopExecutor
from async_anywhere.futures import AwaitableFuture
from tests.test_executors.utils import forked


@contextlib.contextmanager
def running_singleton(**kwargs):
    """`with running_singleton(**kwargs) as singleton:` makes a singleton, and on leaving shuts it down, interrupting."""
    singleton = SingletonEventLoopExecutor(**kwargs)
    try:
        yield singleton
    finally:
        singleton.shutdown(wait=False, cancel_futures=True).result(5)


async def work(result="done", seconds=0.0):
    await asyncio.sleep(seconds)
    return result


async def waits_for(started: AwaitableFuture, release: AwaitableFuture):
    started.set_result(None)
    return await release  # Until the test resolves it


# States


def test_is_stopped_until_it_has_a_worker():
    with running_singleton() as singleton:
        assert singleton.state is singleton.Stopped
        running = singleton.wait_for_state(singleton.Running)
        assert not running.done()
        worker = singleton.get().result(5)
        assert singleton.state is singleton.Running
        assert running.result(0) == (singleton.Stopped, singleton.Running, worker)


def test_runs_at_once_unless_lazy():
    with running_singleton(lazy=False) as singleton:
        _, state, worker = singleton.wait_for_state(singleton.Running).result(0)
        assert state is singleton.Running and worker is singleton.get().result(0)


def test_follows_its_worker_stopping_of_its_own_accord_then_replaces_it():
    with running_singleton(idle_timeout=0.05) as singleton:
        stopping = singleton.wait_for_state(singleton.Stopping)  # Before there's a worker that could expire
        first = singleton.get().result(5)
        _, _, why = singleton.wait_for_state(singleton.Stopped).result(5)
        assert type(why) is CleanStop and why.args == ("(idle) expired",)
        assert stopping.result(0) == (singleton.Running, singleton.Stopping, why)  # The worker's own why
        running = singleton.wait_for_state(singleton.Running)
        assert singleton.submit(work).result(5) == "done"
        assert running.result(0)[2] is not first


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork()")
def test_a_forked_process_has_it_stopped_and_makes_a_worker_of_its_own():
    with running_singleton(lazy=False) as singleton:
        inherited = singleton.get().result(0)
        with forked() as f:
            if f.child:
                _, state, why = singleton.wait_for_state(singleton.Stopped).result(0)
                assert state is singleton.Stopped and type(why) is WorkerForked
                assert singleton.submit(work).result(5) == "done"
                assert singleton.get().result(0) is not inherited
                singleton.shutdown()
        assert singleton.state is singleton.Running and singleton.get().result(0) is inherited  # Unchanged here


# shutdown()


def test_shutdown_without_a_worker_returns_at_once():
    singleton = SingletonEventLoopExecutor()
    stopped = singleton.shutdown()
    assert stopped.done() and stopped.result() == (None, singleton.Stopped, None)


def test_shutdown_waits_for_it_to_follow_the_worker_to_stopped():
    with running_singleton(lazy=False) as singleton:
        worker = singleton.get().result(0)
        shutdown = singleton.shutdown()
        assert shutdown.done() and singleton.state is singleton.Stopped and worker.state is worker.Stopped
        _, _, why = shutdown.result()
        assert shutdown.result() == (singleton.Stopping, singleton.Stopped, why)
        assert type(why) is CleanStop and why.args == ("(idle) manually stopped",)
        assert why is worker.wait_for_state(worker.Stopped).result(0)[2]


def test_after_shutdown_the_next_call_makes_a_new_worker():
    with running_singleton(lazy=False) as singleton:
        first = singleton.get().result(0)
        singleton.shutdown()
        running = singleton.wait_for_state(singleton.Running)
        assert singleton.submit(work).result(5) == "done"
        _, _, second = running.result(0)
        assert second is not first and second is singleton.get().result(0)
        assert singleton.state is singleton.Running


def test_shutdown_lets_calls_in_flight_finish():
    with running_singleton() as singleton:
        started, release = AwaitableFuture(), AwaitableFuture()
        call = singleton.submit(waits_for, started, release)
        started.result(5)
        stopped = singleton.shutdown(wait=False)
        singleton.wait_for_state(singleton.Stopping).result(5)  # Once the stop has begun
        release.set_result("finished")  # Only now, so it was in flight throughout the drain
        assert call.result(5) == "finished"
        _, _, why = stopped.result(5)
        assert type(why) is CleanStop and why.args == ("(waiting for running jobs) manually stopped",)


def test_shutdown_cancelling_futures_interrupts_calls_in_flight():
    with running_singleton() as singleton:
        started = AwaitableFuture()
        call = singleton.submit(waits_for, started, AwaitableFuture())
        started.result(5)
        _, _, why = singleton.shutdown(cancel_futures=True).result()
        with pytest.raises(concurrent.futures.CancelledError):
            call.result(5)
        assert type(why) is WorkerStopped and why.args == ("(interrupted) manually stopped",)


def test_shutdown_cant_wait_on_the_workers_own_thread():
    with running_singleton() as singleton:

        async def stop_waiting():
            singleton.shutdown()  # wait=True

        with pytest.raises(RuntimeError, match="would wait for itself"):
            singleton.submit(stop_waiting).result(5)
        assert singleton.state is singleton.Running  # Refusing to wait stopped nothing


def test_shutdowns_future_refuses_blocking_waits_on_the_workers_own_loop():
    with running_singleton() as singleton:

        async def stop_then_wait():
            singleton.shutdown(wait=False).result()

        with pytest.raises(WouldBlock):
            singleton.submit(stop_then_wait).result(5)
        singleton.wait_for_state(singleton.Stopped).result(5)  # The stop went ahead


@pytest.mark.parametrize("given", ["nothing", "loops", "all loops"])
async def test_state_waits_refuse_blocking_waits_on_the_current_workers_loop_whatever_kwargs_say(given):
    other = asyncio.get_running_loop()  # The test's, never a worker's
    kwargs = {
        "nothing": {},
        "loops": {"prevent_blocking_wait_in_loops": (other,)},
        "all loops": {"prevent_blocking_wait_in_loops": "all"},
    }[given]
    with running_singleton(lazy=False) as singleton:

        async def waits():
            return singleton.wait_for_state(singleton.Stopped, **kwargs).result(1)

        for _ in range(2):  # On the first worker's loop, then on its replacement's
            with pytest.raises(WouldBlock):
                await singleton.submit(waits)
            await singleton.shutdown(wait=False)


def test_state_waits_refuse_blocking_waits_on_a_stopping_workers_loop_while_its_being_replaced():
    with running_singleton(interrupt_stopping_workers=False) as singleton:
        started, go = AwaitableFuture(), AwaitableFuture()

        async def waits_while_draining():
            started.set_result(None)
            await go
            return singleton.wait_for_state(singleton.Stopped).result(1)  # Which this worker stopping moves it to

        call = singleton.submit(waits_while_draining)
        started.result(5)
        singleton.shutdown(wait=False)  # A drain, which the call holds up
        singleton.wait_for_state(singleton.Stopping).result(5)
        replacement = singleton.get()  # Pending until the draining worker has stopped
        assert not replacement.done()
        go.set_result(None)
        with pytest.raises(WouldBlock):
            call.result(5)
        replacement.result(5)


def test_shutdown_while_replacing_a_worker_stops_the_replacement_after_calls_made_to_it():
    release = AwaitableFuture()

    async def main(_):
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await release  # Cleaning up until the test releases it
            raise

    with running_singleton(main=main, interrupt_stopping_workers=False) as singleton:
        first = singleton.get().result(5)
        first.shutdown(wait=False)
        singleton.wait_for_state(singleton.Stopping).result(5)
        # To first's replacement, made once first has stopped
        call = singleton.submit(waits_for, started := AwaitableFuture(), finish := AwaitableFuture())
        stopped = singleton.shutdown(wait=False)
        running = singleton.wait_for_state(singleton.Running)
        assert not (started.done() or stopped.done() or running.done())
        release.set_result(None)
        _, _, second = running.result(5)
        second.wait_for_state(second.Stopping).result(5)  # Stopping with the call in flight
        finish.set_result("made first")
        assert call.result(5) == "made first"
        _, _, why = stopped.result(5)
        assert why.args == ("(waiting for running jobs) manually stopped",)
        assert second is not first and second.state is second.Stopped and singleton.state is singleton.Stopped


async def test_shutdown_can_be_awaited():
    with running_singleton(lazy=False) as singleton:
        _, state, why = await singleton.shutdown(wait=False)
        assert state is singleton.Stopped and type(why) is CleanStop
