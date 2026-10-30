"""EventLoopExecutors in processes forked from their own: see _fork_safety. These tests are synchronous, as a process
forked from a running event loop's thread carries a copy of it that asyncio no longer counts as running."""
from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import os
import select
import threading
import time

import pytest

from async_anywhere.executors import _fork_safety
from async_anywhere.executors.exceptions import CleanStop, WorkerForked, WorkerStopped
from async_anywhere.futures import AwaitableFuture
from tests.test_executors.utils import Worker, bridge, forked, reason, running, stop_begun


pytestmark = pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork()")


class Remembers(Worker):
    @bridge
    async def waits(self):
        try:
            await asyncio.sleep(10)
        finally:
            self.cleaned_up_in = os.getpid()


def test_a_forked_process_cant_use_its_copies_of_workers_nor_wait_for_them():
    worker, other, finished, draining = Remembers(), Worker(), Worker(), Worker()
    finished.shutdown(wait=False).result(2)
    drained = draining.work(10)
    assert type(stop_begun(draining).result(1)[2]) is CleanStop  # Draining as the process forks
    stopping = draining.shutdown(wait=False)  # So still waiting for the drain as the process forks
    in_flight = worker.waits()  # Submitted as made, so in flight as the process forks
    # Bridged before the fork, as callers' functions would be, and called after it
    work, other_work, draining_work = worker.work, other.work, draining.work
    holding, release = AwaitableFuture(), AwaitableFuture()

    def hold():
        # Held as the process forks
        with worker._condition, stopping._condition, in_flight._condition:
            holding.set_result(None)
            release.result(10)

    holder = threading.Thread(target=hold)
    holder.start()
    holding.result(2)
    with forked() as f:
        if f.child:
            pid = os.getpid()
            with pytest.raises(WorkerForked, match=str(pid)):
                work()  # Rather than waiting for the lock, which a thread that isn't here holds
            assert reason(worker) == f"forked into process {pid}"
            with pytest.raises(WorkerForked):
                worker.shutdown(wait=False)
            with pytest.raises(WorkerForked):
                worker.shutdown()
            assert type(in_flight.exception(1)) is WorkerForked  # Rather than timing out: nothing here runs it
            old, _, why = stopping.result(1)  # Likewise
            assert old is draining.Stopping and type(why) is WorkerForked
            with pytest.raises(WorkerForked):
                other_work()
            with pytest.raises(WorkerForked):
                worker.bridge(asyncio.sleep)(0)  # Bridging is never refused, but calling is
            with pytest.raises(WorkerForked):
                draining_work()  # Draining at the fork, so as if running
            with pytest.raises(WorkerForked):
                draining.shutdown()
            assert type(drained.exception(1)) is WorkerForked
            assert reason(draining) == f"forked into process {pid}"
            with pytest.raises(CleanStop):
                finished.work()  # Finished before the fork, so as in the parent
            with pytest.raises(CleanStop):
                finished.bridge(asyncio.sleep)(0)
            assert type(finished.shutdown(wait=False).result(1)[2]) is CleanStop
            assert getattr(worker, "cleaned_up_in", None) != pid  # Cleanup meant for the parent, not run here
            assert len(_fork_safety.workers.instances) == 0  # Nothing in this process need revisit them
        else:
            release.set_result(None)
    holder.join()
    assert worker.work().result(1) == "done"  # The parent's carries on
    worker.shutdown(wait=False, cancel_futures=True).result(2)
    other.shutdown(wait=False).result(2)
    draining.shutdown(wait=False, cancel_futures=True).result(2)
    assert isinstance(drained.exception(1), concurrent.futures.CancelledError)
    assert type(stopping.result(2)[2]) is CleanStop
    assert isinstance(in_flight.exception(1), concurrent.futures.CancelledError)  # Interrupted, here
    assert worker.cleaned_up_in == os.getpid()


class Forks(Worker):
    def __init__(self, other):
        self.other = other
        super().__init__()

    @bridge
    async def forks(self) -> forked:
        with self._condition:
            in_flight = list(self._submitted)  # Before the fork handler fails and drops them
        work = self.work  # Bridged before the fork, so that calling it there reaches the call itself
        with forked(wait=False) as f:  # For the test to check(), rather than blocking the worker's loop
            if f.child:  # Carrying on in this call, on its copy of the worker's loop
                with pytest.raises(WorkerStopped, match="forked"):
                    await work()  # Made and run on the worker's thread, as its forking thread is that here
                assert self.state is self.other.state is self.Stopped
                assert in_flight and all(future.done() for future in in_flight)  # This call's own, for one
                assert not self.in_worker()  # Not on the worker's thread here, though this one was it
                assert not self.reached("stopped").done() and not self.reached("shutdown").done()
        return f


def test_a_call_that_forks_carries_on_only_in_the_parent():
    with running(Worker) as other, running(Forks, other) as worker:
        worker.forks().result(5).check()
        assert worker.work().result(1) == "done"


def test_workers_stay_unusable_in_processes_forked_from_a_fork():
    with running(Worker) as worker:
        work = worker.work  # Bridged before the forks, so that calling it in the grandchild reaches the call itself
        with forked() as f:
            if f.child:
                with forked() as grandchild:
                    if grandchild.child:
                        with pytest.raises(WorkerForked):
                            work()


def test_state_callbacks_dont_run_in_a_forked_process():
    with running(Worker) as worker, forked() as f:
        if f.child:
            with pytest.raises(WorkerForked):
                worker.shutdown()  # Stopped here, by the fork handler, and finished
            assert worker.state is worker.Stopped
            assert not worker.reached("stopped").done() and not worker.reached("shutdown").done()
    assert worker.reached("stopped").done() and worker.reached("shutdown").done()


def test_a_forked_process_can_make_and_use_workers_of_its_own():
    with running(Worker), forked() as f:
        if f.child:
            with running(Worker) as fresh:
                assert fresh.work().result(1) == "done"
            assert fresh.reached("stopped").done() and fresh.reached("shutdown").done()  # Unlike its inherited copy's


def test_a_forked_process_never_logs_for_its_copies_of_workers():
    # A handler whose stream's lock a thread holds as the process forks, blocked writing to a pipe nothing reads
    read, write = os.pipe()
    stream = open(write, "w", buffering=1)
    handler = logging.StreamHandler(stream)
    blocked = threading.Thread(target=stream.buffer.write, args=(b"x" * (1 << 20),))
    with running(Worker) as worker:
        worker.logger.addHandler(handler)
        blocked.start()
        while select.select([], [write], [], 0)[1]:  # Until the pipe is full, so the write blocks holding the lock
            time.sleep(0.001)
        try:
            with forked() as f:
                if f.child:  # The fork handler logged nothing as it stopped the worker, else it would have hung here
                    worker._log(logging.ERROR, "never written")  # Nor does anything the worker logs later
                    assert not worker.logger.disabled  # Though the logger itself is still the user's to use
                    assert reason(worker) == f"forked into process {os.getpid()}"
        finally:
            worker.logger.removeHandler(handler)
            while blocked.is_alive():  # Draining the pipe, so the write finishes
                if select.select([read], [], [], 0.01)[0]:
                    os.read(read, 1 << 16)
            stream.close()
            os.close(read)
