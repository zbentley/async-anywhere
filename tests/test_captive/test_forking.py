"""CaptiveWorkers in processes forked from their own: see _fork_safety. These tests are synchronous, as a process forked
from a running event loop's thread carries a copy of it that asyncio no longer counts as running."""
from __future__ import annotations

import asyncio
import concurrent.futures
import os
import threading

import pytest

from async_anywhere.captive import _fork_safety
from async_anywhere.captive._exceptions import CleanStop, WorkerForked, WorkerStopped
from async_anywhere.futures import AwaitableFuture
from tests.test_captive.utils import TIMING_SENSITIVE, SlowStop, Worker, bridge, forked, running


pytestmark = pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork()")


class Remembers(Worker):
    @bridge
    async def waits(self):
        try:
            await asyncio.sleep(10)
        finally:
            self.cleaned_up_in = os.getpid()


@TIMING_SENSITIVE
def test_a_forked_process_cant_use_its_copies_of_workers_nor_wait_for_them():
    worker, other, slow, finished, draining = Remembers(), Worker(), SlowStop(), Worker(), Worker()
    finished.stop().result(2)
    drained = draining.work(10)
    assert type(draining.stop(wait=False).result(1)) is CleanStop  # Draining as the process forks
    in_flight = worker.waits()  # Submitted as made, so in flight as the process forks
    stopping = slow.stop()  # Still waiting for _run() to clean up as the process forks
    holding, release = AwaitableFuture(), AwaitableFuture()

    def hold():
        with worker._submitting, worker._stopped._condition:  # Held as the process forks
            holding.set_result(None)
            release.result(10)

    holder = threading.Thread(target=hold)
    holder.start()
    holding.result(2)
    with forked() as f:
        if f.child:
            pid = os.getpid()
            with pytest.raises(WorkerForked, match=str(pid)):
                worker.work()  # Rather than waiting for the lock, which a thread that isn't here holds
            assert worker._stopped.result().args[0].endswith(f" forked into process {pid}")
            with pytest.raises(WorkerForked):
                worker.stop(wait=False)
            with pytest.raises(WorkerForked):
                worker.stop()
            assert type(in_flight.exception(1)) is WorkerForked  # Rather than timing out: nothing here runs it
            assert type(stopping.exception(1)) is WorkerForked  # Likewise
            with pytest.raises(WorkerForked):
                other.work()
            with pytest.raises(WorkerForked):
                worker.bridge(asyncio.sleep)  # Best-effort, but so here
            with pytest.raises(WorkerForked):
                draining.work()  # Draining at the fork, so as if running...
            with pytest.raises(WorkerForked):
                draining.stop()
            assert type(drained.exception(1)) is WorkerForked
            assert draining._stopped.result().args == ("(waiting for running jobs) manually stopped",)  # ...but not quite
            assert type(finished.work().exception(1)) is CleanStop  # Finished before the fork, so as in the parent
            assert type(finished.bridge(asyncio.sleep)(0).exception(1)) is CleanStop
            assert type(finished.stop().result(1)) is CleanStop
            assert getattr(worker, "cleaned_up_in", None) != pid  # Cleanup meant for the parent, not run here
            assert not _fork_safety.workers  # Nothing in this process need revisit them
        else:
            release.set_result(None)
    holder.join()
    assert worker.work().result(1) == "done" and not worker._stopped.done()  # The parent's carries on
    worker.stop(interrupt=True).result(2)
    other.stop().result(2)
    draining.stop(interrupt=True).result(2)
    assert isinstance(drained.exception(1), concurrent.futures.CancelledError)
    assert type(stopping.result(2)) is CleanStop
    assert isinstance(in_flight.exception(1), concurrent.futures.CancelledError)  # Interrupted, here
    assert worker.cleaned_up_in == os.getpid()


def test_failing_finished_fails_stopped_too():
    with running() as worker:
        _fork_safety.workers.discard(worker)  # So that, in the child, it's up to this test to fail _finished
        with forked() as f:
            if f.child:
                worker._fail(worker._finished, WorkerForked())
                assert type(worker._stopped.exception()) is WorkerForked


class Forks(Worker):
    def __init__(self, other):
        self.other = other
        super().__init__()

    @bridge
    async def forks(self) -> forked:
        with forked(wait=False) as f:  # For the test to check(), rather than blocking the worker's loop
            if f.child:  # Carrying on in this call, on its copy of the worker's loop
                with pytest.raises(WorkerStopped, match="forked"):
                    await self.work()  # Made and run on the worker's thread, as its forking thread is that here
                assert self._stopped.done() and self.other._stopped.done()
                assert all(future.done() for future in self._pending)  # This call's own, for one
                assert not self.in_worker()  # Not on the worker's thread here, though this one was it
                assert not self.reached("stopped").done() and not self.reached("shutdown").done()
        return f


def test_a_call_that_forks_carries_on_only_in_the_parent():
    with running(Worker) as other, running(Forks, other) as worker:
        worker.forks().result(5).check()
        assert worker.work().result(1) == "done" and not worker._stopped.done()


def test_workers_stay_unusable_in_processes_forked_from_a_fork():
    with running(Worker) as worker, forked() as f:
        if f.child:
            with forked() as grandchild:
                if grandchild.child:
                    with pytest.raises(WorkerForked):
                        worker.work()


def test_hooks_arent_called_in_a_forked_process():
    with running(Worker) as worker, forked() as f:
        if f.child:
            with pytest.raises(WorkerForked):
                worker.stop()  # Stopped here, by the fork handler, and finished
            assert worker._finished.done()
            assert not worker.reached("stopped").done() and not worker.reached("shutdown").done()
    assert worker.reached("stopped").done() and worker.reached("shutdown").done()
