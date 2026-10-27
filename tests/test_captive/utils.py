"""Utilities for CaptiveWorker's tests: harnesses holding workers as code using them would, which tests synchronise with
through AwaitableFutures (awaitable on the tests' loops, waited on with result() from any thread, and not reset by a
fork), settled() to know a worker's loop has run what was queued on it, running() to stop workers and wait for them, and
forked() to run assertions in a forked process."""
from __future__ import annotations

import asyncio
import functools
import os
import signal
import sys
import tempfile
import time
import traceback

import anyio
import pytest

from async_anywhere.captive import CaptiveWorker
from async_anywhere.futures import AwaitableFuture


# For tests whose short timings an overloaded machine can upset: reran on failure, but not after a hang
TIMING_SENSITIVE = pytest.mark.flaky(reruns=2, rerun_except="from pytest-timeout")


class bridge:
    """Bridges a method of a Harness to its worker, as harness.method(...), marking each call as its job starts."""

    def __init__(self, func):
        self.func = func

    def __get__(self, harness, owner=None):
        if harness is None:
            return self
        method = self.func.__get__(harness)

        @functools.wraps(method)
        async def marked(*args, **kwargs):
            harness.mark(method.__name__)
            return await method(*args, **kwargs)

        return harness.worker.bridge(marked)


class Harness:
    """A worker, held as code using CaptiveWorker would hold one, with what tests give it: bridged methods (see bridge),
    _run() as its parent task, on_stopped() and on_shutdown(), and idle_timeout. The worker's attributes are its too.
    By default, _run() awaits runloop, which the test can resolve to end it, and which an interrupt cancels."""

    idle_timeout = float("inf")

    def __init__(self):
        self._marks, self._counts = {}, {}
        self.runloop = AwaitableFuture()
        self.worker = CaptiveWorker(
            parent_task=self._parent,
            on_stopped=self.on_stopped,
            on_shutdown=self.on_shutdown,
            idle_timeout=self.idle_timeout,
        )

    def __getattr__(self, name):
        if name == "worker":  # Not yet set
            raise AttributeError(name)
        return getattr(self.worker, name)

    def reached(self, name, times=1) -> AwaitableFuture:
        """Resolved, with what mark() was given, once `name` has been marked `times` times: bridged methods are marked
        as their jobs start; _run() as it starts ("_run") and ends ("_run ended"); the worker's hooks as it calls them,
        with what they saw ("stopped" and "shutdown"); and anything else as harnesses, or tests, mark() it ("release",
        say, for what a harness awaits until the test releases it)."""
        # Uncancellable, as a cancelled task awaiting one would cancel it for every other waiter. setdefault(), as tests'
        # threads call this too.
        return self._marks.setdefault((name, times), AwaitableFuture(honor_cancel=False))

    def mark(self, name, value=None):
        times = self._counts[name] = self._counts.get(name, 0) + 1  # Each name only ever on one thread
        self.reached(name, times).set_result(value)

    async def _parent(self, worker):
        self.worker = worker  # Before the constructor returns it, as _run() may want it at once
        self.mark("_run")  # Which then runs on to its first await before the worker's loop does anything else
        try:
            await self._run()
        finally:
            self.mark("_run ended")  # In the same step as the parent task's task ends

    async def _run(self):
        await self.runloop

    def on_stopped(self, worker):
        self.mark("stopped", (worker.in_worker(), worker.state))

    def on_shutdown(self, worker):
        self.mark("shutdown", (worker.in_worker(), worker.state, worker._loop.is_closed()))


class Worker(Harness):
    @bridge
    async def work(self, seconds=0.0, result="done"):
        await asyncio.sleep(seconds)
        return result

    @bridge
    async def waits_for(self, future: AwaitableFuture):
        return await future  # Until the test resolves it

    @bridge
    async def nested(self, seconds):
        await self.work()  # Made on the worker's own thread, so this call's nested one ends long before it does
        await asyncio.sleep(seconds)
        return "outer done"


# tidy(), a call, and tidies(), which any task can await, clean up as they're cancelled: awaiting "release", which a
# second cancellation would cut short, then marking "tidied" with how many times their task was asked to cancel
class Tidy(Worker):
    @bridge
    async def tidy(self):
        await self.tidies()

    async def tidies(self):
        self.mark("tidies")
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await self.reached("release")  # Cleanup that awaits. Cancelling this again would cut it short.
            self.mark("tidied", asyncio.current_task().cancelling())
            raise


class BlocksLoop(Worker):
    @bridge
    async def blocks(self, after: AwaitableFuture | None = None):
        if after:
            await after
        self.mark("blocked")
        self.reached("release").result(5)  # Blocking the worker's loop, synchronously, until the test releases it


class SlowStop(Worker):
    async def _run(self):
        try:
            await self.runloop
        except asyncio.CancelledError:
            await asyncio.sleep(0.15)
            raise


def reason(worker: CaptiveWorker | Harness) -> str:
    return worker.wait_for_state(worker.STOPPING, worker.STOPPED).result().args[0]


# Resolved once the worker's loop has run every callback queued before this (a stop's, or a caller's cancellation, say),
# then what those queued in turn (the wakeups of tasks they cancelled): proven, as the loop runs callbacks in order
def settled(worker: CaptiveWorker | Harness) -> AwaitableFuture:
    done = AwaitableFuture()
    worker._loop.call_soon_threadsafe(worker._loop.call_soon, done.set_result, None)
    return done


class running[W: CaptiveWorker | Harness]:
    """`with running(cls, *args, **kwargs) as worker:` (or `async with`) constructs a worker (or harness), and on leaving
    stops it, interrupting, and waits for it to finish, its thread included."""

    def __init__(self, cls: type[W] = CaptiveWorker, /, *args, **kwargs):
        self.worker = cls(*args, **kwargs)

    def __enter__(self) -> W:
        return self.worker

    def __exit__(self, *_):
        self.worker.stop(interrupt=True).result(5)
        self.worker._thread.join(5)
        assert not self.worker._thread.is_alive()

    async def __aenter__(self) -> W:
        return self.worker

    async def __aexit__(self, *_):
        with anyio.fail_after(5):  # So it works on any of anyio's backends
            await self.worker.stop(interrupt=True)
        await anyio.to_thread.run_sync(self.worker._thread.join, 5)
        assert not self.worker._thread.is_alive()


class forked:
    """`with forked() as f:` forks, returning on both sides of the fork, and `if f.child:` runs assertions in the child.

    The child's stdout and stderr (fds 1 and 2, and sys.stdout and sys.stderr) go to a temporary file, as does the
    traceback of whatever leaves the block, and it always exits as it leaves the block, with os._exit(0): pytest's
    machinery mustn't carry on in it. The parent, as it leaves the block, waits for the child (killing it if it hasn't
    exited after `timeout` seconds), and fails the test if it wrote anything, or didn't exit cleanly. With wait=False,
    the parent waits only once it calls check(): a parent forking on a worker's thread shouldn't block its loop, so it
    can return `f` for the test to check(), or check() in a thread."""

    def __init__(self, timeout: float = 10, wait: bool = True):
        self.timeout, self.wait = timeout, wait

    def __enter__(self):
        self._file = tempfile.TemporaryFile(buffering=0)  # Unlike a pipe, never full
        self.pid = os.fork()
        self.child = self.pid == 0
        if self.child:
            os.dup2(self._file.fileno(), 1)
            os.dup2(self._file.fileno(), 2)
            sys.stdout = sys.stderr = open(2, "w", buffering=1, errors="backslashreplace", closefd=False)
        return self

    def __exit__(self, kind, error, tb):
        if not self.child:
            if self.wait:
                self.check()
            return
        try:
            if error is not None:
                sys.stderr.write("".join(traceback.format_exception(error)))
            sys.stderr.flush()
        finally:
            os._exit(0)

    def check(self):
        """Waits for the child, and fails the test if it wrote anything, or didn't exit cleanly: see forked."""
        deadline, hung = time.monotonic() + self.timeout, False
        while not (waited := os.waitpid(self.pid, os.WNOHANG))[0]:
            if hung := time.monotonic() > deadline:
                os.kill(self.pid, signal.SIGKILL)
                waited = os.waitpid(self.pid, 0)
                break
            time.sleep(0.01)
        self._file.seek(0)
        output = self._file.read().decode(errors="replace")
        self._file.close()
        if hung:
            pytest.fail(f"child {self.pid} hung, so was killed after {self.timeout}s, having written:\n{output}")
        if output:
            pytest.fail(f"in child {self.pid}:\n{output}", pytrace=False)
        assert os.waitstatus_to_exitcode(waited[1]) == 0
