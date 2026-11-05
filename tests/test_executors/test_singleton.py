"""SingletonEventLoopExecutor's workers, made as they're needed, each replacement running nothing until the worker it
replaces has finished, and its shutdown()."""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import gc
import os
import signal
import sys
import threading
import time
import weakref

import pytest

from async_anywhere.exceptions import CleanStop, WorkerStopped, WouldBlock
from async_anywhere.executors import EventLoopExecutor, _fork_safety
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


def cleans_up_until(release: AwaitableFuture, ran: list | None = None):
    """A main that runs until cancelled, then cleans up until release is resolved, adding its worker to ran."""

    async def main(worker):
        if ran is not None:
            ran.append(worker)
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await release
            raise

    return main


# Workers


@pytest.mark.parametrize("lazy", [True, False])
def test_makes_its_first_worker_on_the_first_get_unless_lazy(lazy):
    with running_singleton(lazy=lazy) as singleton:
        assert (singleton.shutdown().result(0) is None) is lazy  # Stopping the worker made at once, if any
        worker = singleton.get()
        assert worker.state is worker.Running and singleton.get() is worker


def test_replaces_a_worker_that_stopped_of_its_own_accord():
    with running_singleton(idle_timeout=0.05) as singleton:
        first = singleton.get()
        why = first.wait_for_state(first.Shutdown).result(5)
        assert type(why) is CleanStop and why.args == ("(idle) expired",)
        assert singleton.submit(work).result(5) == "done"
        assert singleton.get() is not first


def test_a_replacement_runs_nothing_until_the_worker_it_replaces_has_finished():
    release, first, seen = AwaitableFuture(), None, []

    def saw(what):  # And how far along the first worker was, as it ran
        seen.append((what, first.state, first._thread.is_alive()))

    async def main(worker):
        if first is not None and worker is not first:
            saw("main")
        await cleans_up_until(release)(worker)

    async def call():
        saw("call")

    # Not interrupting first again as it's replaced, which would cut its cleanup short
    with running_singleton(main=main, interrupt_stopping_workers=False) as singleton:
        first = singleton.get()
        first.shutdown(wait=False)  # Its main cleaning up until released
        first.wait_for_state(first.Stopping).result(5)
        second = singleton.get()  # Made, and handed out, at once
        assert second is not first and second.state is second.Running
        made = singleton.submit(call)
        release.set_result(None)
        made.result(5)
        assert seen == [("main", first.Shutdown, False), ("call", first.Shutdown, False)]


def test_a_replacements_idle_countdown_starts_once_it_runs():
    release, began, first = AwaitableFuture(), AwaitableFuture(), None
    timeout = 0.2

    async def main(worker):
        if first is not None and worker is not first:
            began.set_result(time.monotonic())
        await cleans_up_until(release)(worker)

    with running_singleton(main=main, idle_timeout=timeout, interrupt_stopping_workers=False) as singleton:
        first = singleton.get()
        first.shutdown(wait=False)
        first.wait_for_state(first.Stopping).result(5)
        second = singleton.get()
        time.sleep(timeout * 2)  # Longer than its idle timeout, waiting for first: which it doesn't count
        release.set_result(None)
        why = second.wait_for_state(second.Stopping).result(5)
        assert time.monotonic() - began.result(0) >= timeout * 0.9  # Rather than expiring as it began
        assert type(why) is CleanStop and why.args == ("(idle) expired",)


@pytest.mark.parametrize("waits_for", ["a call", "a state"])
def test_a_stopping_workers_calls_go_to_its_replacement_but_cant_wait_for_it_there(waits_for):
    with running_singleton(interrupt_stopping_workers=False) as singleton:
        started, go, made = AwaitableFuture(), AwaitableFuture(), []

        async def drains():
            started.set_result(None)
            await go
            replacement = singleton.get()  # Made here, as this worker is stopping
            made.append(singleton.submit(work))  # To it, though it runs nothing until this worker has finished
            waited = made[0] if waits_for == "a call" else replacement.wait_for_state(replacement.Shutdown)
            return replacement, waited.result(1)

        first = singleton.get()
        call = singleton.submit(drains)
        started.result(5)
        first.shutdown(wait=False)  # A drain, which the call holds up
        first.wait_for_state(first.Stopping).result(5)
        go.set_result(None)
        with pytest.raises(WouldBlock):
            call.result(5)
        assert made[0].result(5) == "done"  # Once first had finished
        assert singleton.get() is not first


def test_workers_it_has_replaced_are_let_go():
    with running_singleton() as singleton:
        replaced = []
        for _ in range(3):
            worker = singleton.get()
            replaced.append(weakref.ref(worker))
            worker.shutdown()
            worker._thread.join()  # Past its last step, once its state has changed for the last time
        del worker
        gc.collect()
        assert [ref() is None for ref in replaced] == [True, True, False]  # The last kept, until get() replaces it


def test_callbacks_on_a_workers_thread_as_it_shuts_down_cant_wait_for_its_replacement():
    with running_singleton() as singleton:
        first, seen = singleton.get(), AwaitableFuture()

        def waits(_):  # On first's thread, which its replacement waits for to finish
            try:
                singleton.submit(work).result(1)
            except BaseException as error:
                seen.set_result(type(error))
            else:
                seen.set_result(None)

        shutdown = first.wait_for_state(first.Shutdown)  # Held, as its waiter set holds it only weakly
        shutdown.add_done_callback(waits)
        first.shutdown(wait=False)
        assert seen.result(5) is WouldBlock


@pytest.mark.skipif(not hasattr(signal, "pthread_kill"), reason="Interrupts the main thread with signal.pthread_kill()")
def test_a_worker_whose_get_was_interrupted_as_it_was_made_runs_nothing():
    main_ident, release, ran = threading.main_thread().ident, AwaitableFuture(), []

    class InterruptsItsMaker(asyncio.SelectorEventLoop):
        interrupted = False

        def run_until_complete(self, future):
            if not self.interrupted:  # Its first run, which the constructor waits for to start
                self.interrupted = True
                waiting = EventLoopExecutor.current()._runner._condition
                # Not until main is blocked in Condition.wait()'s try, which only run() below can wake it from: there, the
                # interrupt comes from its acquire(), and the finally restores the lock, which elsewhere it can unbalance
                while not (
                    (frame := sys._current_frames()[main_ident]).f_code is threading.Condition.wait.__code__
                    and frame.f_locals["self"] is waiting
                    and "gotit" in frame.f_locals
                ):
                    time.sleep(0.001)
                signal.pthread_kill(main_ident, signal.SIGINT)  # As Ctrl+C would, as get() makes it
            return super().run_until_complete(future)

    made = []

    def loop_factory():
        made.append(None)
        return InterruptsItsMaker() if len(made) == 2 else asyncio.new_event_loop()

    with running_singleton(
        main=cleans_up_until(release, ran), loop_factory=loop_factory, interrupt_stopping_workers=False
    ) as singleton:
        first = singleton.get()
        first.shutdown(wait=False)  # Its main cleaning up until released
        first.wait_for_state(first.Stopping).result(5)
        before = set(_fork_safety.workers.instances)
        with pytest.raises(KeyboardInterrupt):
            singleton.get()
        [interrupted] = set(_fork_safety.workers.instances) - before
        third = singleton.get()  # Waiting for first too, as the interrupted one does
        release.set_result(None)
        interrupted.wait_for_state(interrupted.Shutdown).result(5)
        assert singleton.submit(work).result(5) == "done"
        assert ran == [first, third] and singleton.get() is third


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork()")
def test_a_forked_process_makes_a_worker_of_its_own():
    with running_singleton(lazy=False) as singleton:
        inherited = singleton.get()
        with forked() as f:
            if f.child:
                assert inherited.state is inherited.Shutdown
                assert singleton.submit(work).result(5) == "done"
                assert singleton.get() is not inherited
                singleton.shutdown()
        assert inherited.state is inherited.Running and singleton.get() is inherited  # Unchanged here


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork()")
def test_a_forked_process_can_use_it_though_a_thread_that_isnt_there_held_its_lock():
    with running_singleton(lazy=False) as singleton:
        holding, release = AwaitableFuture(), AwaitableFuture()

        def hold():
            with singleton._worker:  # Held as the process forks
                holding.set_result(None)
                release.result(10)

        holder = threading.Thread(target=hold)
        holder.start()
        holding.result(2)
        try:
            with forked() as f:
                if f.child:
                    assert singleton.submit(work).result(5) == "done"  # Rather than waiting for a lock no one here holds
                    singleton.shutdown()
        finally:
            release.set_result(None)
            holder.join()


# shutdown()


def test_shutdown_without_a_worker_returns_at_once():
    stopped = SingletonEventLoopExecutor().shutdown()
    assert stopped.done() and stopped.result() is None


def test_shutdown_waits_for_the_worker_to_shut_down():
    with running_singleton(lazy=False) as singleton:
        worker = singleton.get()
        shutdown = singleton.shutdown()
        assert shutdown.done() and worker.state is worker.Shutdown
        why = shutdown.result()
        assert type(why) is CleanStop and why.args == ("(idle) manually stopped",)
        assert why is worker.wait_for_state(worker.Shutdown).result(0)


def test_after_shutdown_the_next_call_makes_a_new_worker():
    with running_singleton(lazy=False) as singleton:
        first = singleton.get()
        singleton.shutdown()
        assert singleton.submit(work).result(5) == "done"
        second = singleton.get()
        assert second is not first and second.state is second.Running


def test_shutdown_lets_calls_in_flight_finish():
    with running_singleton() as singleton:
        started, release = AwaitableFuture(), AwaitableFuture()
        call = singleton.submit(waits_for, started, release)
        started.result(5)
        worker = singleton.get()
        stopped = singleton.shutdown(wait=False)
        worker.wait_for_state(worker.Stopping).result(5)  # Once the stop has begun
        release.set_result("finished")  # Only now, so it was in flight throughout the drain
        assert call.result(5) == "finished"
        why = stopped.result(5)
        assert type(why) is CleanStop and why.args == ("(waiting for running jobs) manually stopped",)


def test_shutdown_cancelling_futures_interrupts_calls_in_flight():
    with running_singleton() as singleton:
        started = AwaitableFuture()
        call = singleton.submit(waits_for, started, AwaitableFuture())
        started.result(5)
        why = singleton.shutdown(cancel_futures=True).result()
        with pytest.raises(concurrent.futures.CancelledError):
            call.result(5)
        assert type(why) is WorkerStopped and why.args == ("(interrupted) manually stopped",)


def test_shutdown_cant_wait_on_the_workers_own_thread():
    with running_singleton() as singleton:

        async def stop_waiting():
            singleton.shutdown()  # wait=True

        worker = singleton.get()
        with pytest.raises(RuntimeError, match="would wait for itself"):
            singleton.submit(stop_waiting).result(5)
        assert worker.state is worker.Running and singleton.get() is worker  # Refusing to wait stopped nothing


def test_shutdowns_future_refuses_blocking_waits_on_the_workers_own_loop():
    with running_singleton() as singleton:

        async def stop_then_wait():
            singleton.shutdown(wait=False).result()

        worker = singleton.get()
        with pytest.raises(WouldBlock):
            singleton.submit(stop_then_wait).result(5)
        worker.wait_for_state(worker.Shutdown).result(5)  # The stop went ahead


def test_shutdown_on_a_worker_it_has_replaced_stops_that_one_and_makes_none():
    release, go, called = AwaitableFuture(), AwaitableFuture(), AwaitableFuture()

    async def main(worker):
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await go
            called.set_result_if_not_done(singleton.shutdown(wait=False))  # Here, on a worker it has replaced
            await release
            raise

    with running_singleton(main=main, interrupt_stopping_workers=False) as singleton:
        first = singleton.get()
        first.shutdown(wait=False)  # Its main cleaning up until released
        first.wait_for_state(first.Stopping).result(5)
        second = singleton.get()
        go.set_result(None)
        stopping = called.result(5)
        release.set_result(None)
        assert stopping.result(5) is first.wait_for_state(first.Shutdown).result(0)  # first's, not second's
        assert singleton.submit(work).result(5) == "done"
        assert singleton.get() is second and second.state is second.Running  # Never stopped, nor replaced


def test_shutdown_while_replacing_a_worker_stops_the_replacement_after_calls_made_to_it():
    release = AwaitableFuture()

    with running_singleton(main=cleans_up_until(release), interrupt_stopping_workers=False) as singleton:
        first = singleton.get()
        first.shutdown(wait=False)
        first.wait_for_state(first.Stopping).result(5)
        second = singleton.get()  # Running nothing until first has finished
        call = singleton.submit(waits_for, started := AwaitableFuture(), finish := AwaitableFuture())
        stopped = singleton.shutdown(wait=False)  # second's
        assert not (started.done() or stopped.done())
        release.set_result(None)
        second.wait_for_state(second.Stopping).result(5)  # Stopping with the call in flight
        finish.set_result("made first")
        assert call.result(5) == "made first"
        why = stopped.result(5)
        assert why.args == ("(waiting for running jobs) manually stopped",)
        assert second is not first and second.state is second.Shutdown


async def test_shutdown_can_be_awaited():
    with running_singleton(lazy=False) as singleton:
        worker = singleton.get()
        why = await singleton.shutdown(wait=False)
        assert worker.state is worker.Shutdown and type(why) is CleanStop
