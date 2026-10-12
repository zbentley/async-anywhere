import asyncio
import logging
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from itertools import count
from typing import ClassVar

import attr
import pytest

from async_anywhere.captive import BackgroundThreadWorker, register_bridge_method, _CleanStop

pytestmark = pytest.mark.filterwarnings("error::pytest.PytestUnhandledThreadExceptionWarning")

_IDLE = 0.2
_MODES = pytest.mark.parametrize("blocking", [False, True], ids=["async", "blocking"])


async def _pause(seconds: float, blocking: bool):
    if blocking:
        time.sleep(seconds)  # Holds the worker's whole loop, like a synchronous client call would
    else:
        await asyncio.sleep(seconds)


def _wait_until(predicate, timeout=2.0) -> bool:
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.01)
    return predicate()


def _stop_reason(worker: BackgroundThreadWorker, timeout=2.0) -> BaseException:
    return worker._stopped.exception(timeout)


class _Client:
    """Stands in for a third-party client that has to live on the worker's own loop."""

    def __init__(self, worker: _Worker):
        self.worker = worker
        self.loop = asyncio.get_running_loop()
        self.thread = threading.current_thread()
        self.started, self.finished, self.cancelled = [], [], []

    async def itself(self) -> _Client:
        return self

    async def work(self, label, seconds=0.0, blocking=False):
        assert asyncio.get_running_loop() is self.loop
        self.started.append(label)
        try:
            await _pause(seconds, blocking)
        except asyncio.CancelledError:
            self.cancelled.append(label)
            raise
        self.finished.append(label)
        return label

    async def fail(self, exc: BaseException):
        raise exc

    async def reenter(self, label):
        return await self.worker.work(label)


@attr.define(eq=False)
class _Worker(BackgroundThreadWorker[_Client]):
    name: str = ""
    startup_seconds: float = 0.0
    blocking_startup: bool = False
    startup_error: str | None = None
    run_seconds: float | None = None  # None: run until stopped
    cleanup_seconds: float = 0.0
    clients: list[_Client] = attr.field(init=False, factory=list)

    async def _run(self):
        await _pause(self.startup_seconds, self.blocking_startup)
        if self.startup_error:
            raise ValueError(self.startup_error)
        self.clients.append(client := _Client(self))
        self._report_ready(client)
        try:
            if self.run_seconds is None:
                while True:
                    await asyncio.sleep(1)
            await asyncio.sleep(self.run_seconds)
        finally:
            await asyncio.sleep(self.cleanup_seconds)

    itself = register_bridge_method(_Client.itself)
    work = register_bridge_method(_Client.work)
    fail = register_bridge_method(_Client.fail)
    reenter = register_bridge_method(_Client.reenter)


@attr.define(eq=False)
class _IdleWorker(_Worker):
    _idle_timeout = _IDLE


@attr.define(eq=False)
class _FastIdleWorker(_Worker):
    _idle_timeout = 0.03


@attr.define(eq=False)
class _StubbornWorker(_Worker):
    stubborn: bool = attr.field(init=False, default=True)

    async def _run(self):
        self._report_ready(_Client(self))
        while self.stubborn:
            with suppress(asyncio.CancelledError):
                await asyncio.sleep(0.05)


@attr.define(eq=False)
class _DeadOnArrivalWorker(_Worker):
    built: ClassVar[list] = []

    def __attrs_post_init__(self):
        self.built.append(self)
        super().__attrs_post_init__()
        self.unsafe_shut_down()


@attr.define(eq=False)
class _LateWorker(_Worker):
    pass


@pytest.fixture(autouse=True)
def _quiet_machinery(caplog):
    """asyncio or concurrent.futures logging anything means a bridged future or callback misbehaved."""
    caplog.set_level(logging.WARNING)
    yield
    assert not [f"{r.name}: {r.getMessage()}" for r in caplog.records if r.name in ("asyncio", "concurrent.futures")]


# ---------------------------------------------------------------- startup and caching


def test_get_caches_per_class_and_arguments():
    worker = _Worker.get(name="a", startup_seconds=0)
    assert _Worker.get(startup_seconds=0, name="a") is worker
    assert _Worker.get(name="b") is not worker
    assert _IdleWorker.get(name="a", startup_seconds=0) is not worker


def test_concurrent_get_builds_one_worker():
    with ThreadPoolExecutor(8) as pool:
        workers = set(pool.map(lambda _: _Worker.get(name="shared"), range(16)))
    assert len(workers) == 1


@_MODES
async def test_calls_wait_for_ready(blocking):
    start = time.monotonic()
    worker = _Worker.get(startup_seconds=0.2, blocking_startup=blocking)
    assert await worker.work("x") == "x"
    assert time.monotonic() - start >= 0.2


def test_get_replaces_dead_cached_worker(monkeypatch):
    worker = _Worker.get()
    with monkeypatch.context() as m:
        # As if the worker died before its cache entry was stored, so its own eviction found nothing to evict.
        m.setattr(BackgroundThreadWorker._CACHE, "invalidate_value", lambda value: 0)
        worker.unsafe_shut_down()
    replacement = _Worker.get()
    assert replacement is not worker and not replacement._stopped.done()


def test_get_retries_dead_worker_only_once():
    _DeadOnArrivalWorker.built.clear()
    with pytest.raises(_CleanStop, match="manually stopped"):
        _DeadOnArrivalWorker.get()
    assert len(_DeadOnArrivalWorker.built) == 2


def test_loop_that_starts_late_is_cleaned_up(monkeypatch):
    real_run = asyncio.run

    def late_run(coro):
        time.sleep(2.3)
        return real_run(coro)

    with monkeypatch.context() as m:
        m.setattr(asyncio, "run", late_run)
        with pytest.raises(TimeoutError, match="did not start"):
            _LateWorker.get(startup_seconds=1)
    # Left running, it would report ready ~1s after its loop came up and then run forever.
    assert _wait_until(lambda: not any(t.name == "maffo.worker[_LateWorker]" for t in threading.enumerate()), 2)


# ---------------------------------------------------------------- bridged calls


@_MODES
async def test_bridged_call_runs_on_the_worker(blocking):
    worker = _Worker.get()
    assert await worker.work("x", 0.01, blocking) == "x"
    client = await worker.itself()
    assert client.thread is worker._thread and client.thread is not threading.current_thread()
    assert client.loop is worker._runner and client.loop is not asyncio.get_running_loop()
    assert client.finished == ["x"]


async def test_bridged_call_can_reenter_the_worker():
    worker = _Worker.get()
    assert await asyncio.wait_for(worker.reenter("again"), 1) == "again"


@pytest.mark.parametrize(
    "blocking, serialized", [(False, False), (True, True)], ids=["async-calls-interleave", "blocking-calls-serialize"]
)
async def test_calls_share_one_worker_loop(blocking, serialized):
    worker = _Worker.get()
    await worker.work("warmup")
    start = time.monotonic()
    await asyncio.gather(*(worker.work(i, 0.1, blocking) for i in range(3)))
    elapsed = time.monotonic() - start
    assert elapsed >= 0.3 if serialized else elapsed < 0.25


def test_calls_from_many_threads_and_loops():
    worker = _Worker.get()

    async def burst(n):
        return await asyncio.gather(*(worker.work((n, i), 0.01) for i in range(10)))

    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(lambda n: asyncio.run(burst(n)), range(8)))
    assert results == [[(n, i) for i in range(10)] for n in range(8)]
    assert worker._expire_tracker.calls == 0
    assert len(worker._stopped._done_callbacks) == 1  # Just _on_stop: abandoned awaiters don't pile up


@pytest.mark.parametrize(
    "raised, expected, match",
    [
        (ValueError("boom"), ValueError, "boom"),
        (KeyError("k"), KeyError, "k"),
        (asyncio.CancelledError(), RuntimeError, "raised CancelledError"),
    ],
    ids=["value-error", "key-error", "own-cancellation"],
)
async def test_bridged_errors_reach_the_caller(raised, expected, match):
    worker = _Worker.get()
    with pytest.raises(expected, match=match):
        await asyncio.wait_for(worker.fail(raised), 1)
    assert await worker.work("after") == "after"
    assert worker._expire_tracker.calls == 0


@_MODES
async def test_cancelling_caller_leaves_job_running(blocking):
    worker = _Worker.get()
    client = await worker.itself()
    caller = asyncio.create_task(worker.work("job", 0.2, blocking))
    assert await asyncio.to_thread(_wait_until, lambda: "job" in client.started)
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    assert await asyncio.to_thread(_wait_until, lambda: client.finished == ["job"])
    assert client.cancelled == [] and not worker._stopped.done()
    assert await worker.work("next") == "next"


# ---------------------------------------------------------------- idle expiry


async def test_idle_worker_expires_and_is_replaced():
    worker = _IdleWorker.get()
    await worker.work("x")
    start = time.monotonic()
    reason = await asyncio.to_thread(_stop_reason, worker)
    assert type(reason) is _CleanStop and "(idle) expired" in str(reason)
    assert _IDLE * 0.75 <= time.monotonic() - start <= _IDLE * 3
    assert await asyncio.to_thread(worker._thread.join, 2) is None and not worker._thread.is_alive()
    assert _IdleWorker.get() is not worker


@_MODES
async def test_calls_postpone_expiry(blocking):
    worker = _IdleWorker.get()
    assert await worker.work("long", _IDLE * 3, blocking) == "long"
    assert not worker._stopped.done()
    assert "(idle) expired" in str(await asyncio.to_thread(_stop_reason, worker))


async def test_worker_without_idle_timeout_never_expires():
    worker = _Worker.get()
    await worker.work("x")
    await asyncio.sleep(_IDLE * 3)
    assert not worker._stopped.done()


def test_expiry_never_interrupts_a_started_call():
    """Callers racing a fast-expiring worker may be refused, but never once their call has started."""
    refused, problems, lock = [], [], threading.Lock()
    deadline = time.monotonic() + 1.5

    async def caller(n):
        for i in count():
            if time.monotonic() > deadline:
                return
            worker = _FastIdleWorker.get()
            await asyncio.sleep(random.random() * 0.08)  # Sometimes outlive the idle timeout before calling
            label = (n, i)
            try:
                await asyncio.wait_for(worker.work(label, random.random() * 0.02), 2)
            except _CleanStop as ex:
                with lock:
                    refused.append(label)
                    if "(idle) expired" not in str(ex) or any(label in c.started for c in worker.clients):
                        problems.append((label, ex))
            except Exception as ex:
                with lock:
                    problems.append((label, ex))
            await asyncio.sleep(random.random() * 0.06)

    with ThreadPoolExecutor(3) as pool:
        list(pool.map(lambda n: asyncio.run(caller(n)), range(3)))
    assert not problems
    assert refused, "never raced an expiry, so this exercised nothing"


# ---------------------------------------------------------------- stopping


@pytest.mark.parametrize(
    "state, expected, reason",
    [
        ("idle", _CleanStop, "(idle) manually stopped"),
        ("in flight", RuntimeError, "(interrupted) manually stopped"),
        ("starting", _CleanStop, "(idle) manually stopped"),
    ],
    ids=["idle", "in-flight", "starting"],
)
async def test_unsafe_shut_down(state, expected, reason):
    worker = _Worker.get(startup_seconds=1 if state == "starting" else 0)
    if state == "idle":
        await worker.work("warmup")
    job = asyncio.create_task(worker.work("job", 5))
    if state == "in flight":
        assert await asyncio.to_thread(_wait_until, lambda: worker.clients and "job" in worker.clients[0].started)
    else:
        job.cancel()

    err = await asyncio.to_thread(worker.unsafe_shut_down)
    assert type(err) is expected and reason in str(err)
    assert not worker._thread.is_alive() and worker._runner.is_closed()
    if state == "in flight":
        with pytest.raises(expected, match=re.escape(reason)):
            await asyncio.wait_for(job, 1)
        assert worker.clients[0].cancelled == ["job"]
    if state == "starting":
        assert worker.clients == []  # Startup was abandoned

    start = time.monotonic()
    with pytest.raises(expected):
        await worker.work("too late")
    assert time.monotonic() - start < 0.1


def test_unsafe_shut_down_waits_for_cleanup():
    worker = _Worker.get(cleanup_seconds=0.3)
    worker._value.result(2)
    start = time.monotonic()
    err = worker.unsafe_shut_down()
    assert time.monotonic() - start >= 0.25 and not worker._thread.is_alive()
    assert worker.unsafe_shut_down() is err


def test_unsafe_shut_down_times_out_when_run_ignores_cancellation():
    worker = _StubbornWorker.get()
    with pytest.raises(TimeoutError, match="didn't stop"):
        worker.unsafe_shut_down(timeout_seconds=0.3)
    worker.stubborn = False
    assert "manually stopped" in str(worker.unsafe_shut_down())


def _cancel_worker_tasks(worker: _Worker):
    worker._runner.call_soon_threadsafe(lambda: [task.cancel() for task in asyncio.all_tasks()])


@pytest.mark.parametrize(
    "config, trigger, expected, match",
    [
        ({"run_seconds": 0.1}, None, _CleanStop, r"_run\(\) returned"),
        ({"startup_error": "boom"}, None, ValueError, "boom"),
        ({}, _cancel_worker_tasks, RuntimeError, "CancelledError"),
    ],
    ids=["run-returns", "run-raises", "cancelled-by-someone-else"],
)
async def test_unexpected_stops_reach_callers(config, trigger, expected, match):
    worker = _Worker.get(**config)
    if trigger:
        await worker.work("warmup")
        trigger(worker)
    reason = await asyncio.to_thread(_stop_reason, worker)
    assert type(reason) is expected
    with pytest.raises(expected, match=match):
        await asyncio.wait_for(worker.work("x"), 1)
    assert await asyncio.to_thread(_wait_until, lambda: not worker._thread.is_alive())
    assert _Worker.get(**config) is not worker
