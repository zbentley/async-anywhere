from __future__ import annotations

import argparse
import asyncio
import functools
import os
import statistics
import threading
import time
from collections.abc import Callable
from typing import Any, NamedTuple

from asgiref.sync import AsyncSingleThreadContext, SyncToAsync, async_to_sync, sync_to_async

from async_anywhere.captive import CaptiveWorker


DELAY = 0.01
monotonic = time.monotonic


async def amonotonic():
    return monotonic()


class Workload(NamedTuple):
    name: str
    sync: Callable[..., Any]
    async_: Callable[..., Any]
    args: tuple
    baseline: float
    n: int
    controls: dict[str, Callable[[Workload], list[float]]]


def timed(call, args, n):
    perf_counter = time.perf_counter
    call(*args)  # Warm-up
    timings = [0.0] * n
    for i in range(n):
        start = perf_counter()
        call(*args)
        timings[i] = perf_counter() - start
    return timings


def timed_result(call, args, n):
    perf_counter = time.perf_counter
    call(*args).result()  # Warm-up
    timings = [0.0] * n
    for i in range(n):
        start = perf_counter()
        call(*args).result()
        timings[i] = perf_counter() - start
    return timings


async def timed_async(call, args, n):
    perf_counter = time.perf_counter
    await call(*args)  # Warm-up
    timings = [0.0] * n
    for i in range(n):
        start = perf_counter()
        await call(*args)
        timings[i] = perf_counter() - start
    return timings


def bare_sync(w):
    return timed(w.sync, w.args, w.n)


def bare_asyncio(w):
    return asyncio.run(timed_async(w.async_, w.args, w.n))


def bare_asyncio_yield_once(w, schedule):
    async def run():
        loop = asyncio.get_running_loop()
        create_future, schedule_on_loop = loop.create_future, getattr(loop, schedule)

        async def yield_once():
            # Suspends once: the handle sets the future, whose done callback (queued with call_soon) resumes the task
            future = create_future()
            schedule_on_loop(future.set_result, None)
            await future
            return monotonic()

        return await timed_async(yield_once, w.args, w.n)

    return asyncio.run(run())


def asgiref_async_to_sync(w):
    # A new thread and loop (asyncio.run()) per call
    return timed(async_to_sync(w.async_), w.args, w.n)


def asgiref_async_to_sync_single_thread(w):
    # A thread reused across calls, but still a new loop per call
    with AsyncSingleThreadContext():
        return timed(async_to_sync(w.async_), w.args, w.n)


def asgiref_async_to_sync_cached_loop(w):
    # asgiref only reuses a loop it finds through the threadlocal that sync_to_async() sets: pointed at one of our own
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    threadlocal = SyncToAsync.threadlocal
    threadlocal.main_event_loop, threadlocal.main_event_loop_pid = loop, os.getpid()
    try:
        return timed(async_to_sync(w.async_), w.args, w.n)
    finally:
        del threadlocal.main_event_loop, threadlocal.main_event_loop_pid
        loop.call_soon_threadsafe(loop.stop)
        thread.join()
        loop.close()


def asgiref_sync_to_async(w):
    # Already runs on the caller's loop, and a single cached executor thread: there's no loop to cache
    return asyncio.run(timed_async(sync_to_async(w.sync), w.args, w.n))


def captive_sync_caller(w):
    worker = CaptiveWorker()
    try:
        return timed_result(worker.bridge(w.async_), w.args, w.n)
    finally:
        worker.stop().result()


def captive_asyncio_caller(w):
    worker = CaptiveWorker()
    try:
        return asyncio.run(timed_async(worker.bridge(w.async_), w.args, w.n))
    finally:
        worker.stop().result()


CONTROLS = {
    "control: sync": bare_sync,
    "control: asyncio": bare_asyncio,
}
YIELD_CONTROLS = {
    "control: asyncio, call_soon": functools.partial(bare_asyncio_yield_once, schedule="call_soon"),
    "control: asyncio, call_soon_threadsafe": functools.partial(
        bare_asyncio_yield_once, schedule="call_soon_threadsafe"
    ),
}
BENCHMARKS = {
    "asgiref async_to_sync": asgiref_async_to_sync,
    "asgiref async_to_sync, AsyncSingleThreadContext": asgiref_async_to_sync_single_thread,
    "asgiref async_to_sync, cached loop": asgiref_async_to_sync_cached_loop,
    "asgiref sync_to_async": asgiref_sync_to_async,
    "CaptiveWorker, sync caller": captive_sync_caller,
    "CaptiveWorker, asyncio caller": captive_asyncio_caller,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-n", "--iterations", type=int, default=50)
    parser.add_argument("--noop-iterations", type=int, default=1000)
    args = parser.parse_args()
    workloads = (
        Workload(
            f"time.sleep/asyncio.sleep({DELAY})", time.sleep, asyncio.sleep, (DELAY,), DELAY, args.iterations, CONTROLS
        ),
        Workload("monotonic()", monotonic, amonotonic, (), 0.0, args.noop_iterations, {**CONTROLS, **YIELD_CONTROLS}),
    )

    width = max(map(len, {**CONTROLS, **YIELD_CONTROLS, **BENCHMARKS}))
    columns = ("min", "median", "mean", "p95", "max")
    for w in workloads:
        print(f"\n{w.name}: overhead per call beyond {w.baseline}s, in microseconds, over {w.n} calls each")
        print(f"{'':<{width}}" + "".join(f"{c:>12}" for c in columns))
        for name, benchmark in {**w.controls, **BENCHMARKS}.items():
            overheads = sorted((t - w.baseline) * 1e6 for t in benchmark(w))
            stats = (
                overheads[0],
                statistics.median(overheads),
                statistics.fmean(overheads),
                overheads[min(w.n - 1, round(0.95 * (w.n - 1)))],
                overheads[-1],
            )
            print(f"{name:<{width}}" + "".join(f"{s:>12.2f}" for s in stats))


if __name__ == "__main__":
    main()
