from asyncio import wrap_future, wait, FIRST_COMPLETED, ensure_future, gather as asyncio_gather
from concurrent.futures import Future as ConcurrentFuture
from contextlib import AbstractContextManager
from inspect import iscoroutinefunction
from sys import exc_info
from threading import RLock, Lock
from typing import Awaitable, AsyncGenerator, TYPE_CHECKING, Iterable, Collection

if TYPE_CHECKING:  # async_anywhere.futures imports this module
    from async_anywhere.futures import AwaitableFuture

FAST_COLLECTION_SET_CUTOFF = 5

def validate_coroutine_function(_, attribute, value):
    if not iscoroutinefunction(value):
        raise TypeError(f"{attribute.name} must be a coroutine function (an async def), not {value!r}")

def fast_collection[T](items: Iterable[T]) -> Collection[T]:
    items = tuple(items)
    return frozenset(items) if len(items) > FAST_COLLECTION_SET_CUTOFF else items

class LockProtected[T](AbstractContextManager[T]):
    __slots__ = ("_lock", "_value")

    def __init__(self, initial: T, recursive=False):
        self._lock = RLock() if recursive else Lock()
        self._value = initial

    def __exit__(self, *_):
        self._lock.release()

    def __enter__(self):
        self._lock.acquire()
        return self._value


async def gather(*coros: Awaitable, return_exceptions: bool = False) -> tuple:
    """asyncio.gather isn't a true async method, which makes one-off wrapping it in async_to_sync not work, and also
    confuses typecheckers. We wrap gather in a regular async method that behaves identically to deal with this."""
    if not len(coros):
        return ()
    return tuple(await asyncio_gather(*coros, return_exceptions=return_exceptions))


async def gather_stream[T](*coros: Awaitable[T]) -> AsyncGenerator[T]:
    for result in await gather(*coros):
        for item in result:
            yield item

def raise_errors(*args: Exception):
    if len(args) == 0:
        return
    _, exc, _ = exc_info()
    if len(args) == 1:
        exc = args[0]
    else:
        exc = ExceptionGroup("Multiple errors occurred", args)
    try:
        if exc_info()[1]:
            raise exc from exc_info()[1]
        raise exc
    finally:
        # Prevent weird stack display/GC behavior from exceptions being kept around
        del args
        del exc


async def race(*args: AwaitableFuture | ConcurrentFuture | Awaitable, timeout: float | None = None) -> tuple:
    """Helper for waiting on multiple future-ish things that does the following:
    - Converts AwaitableFuture and concurrent.Future to asyncio.Future.
    - Race-waits for the first one(s) to be completed.
    - Cancels any unfinished futures (which is a noop for AwaitableFuture).
    - Waits for all finished futures (raising exceptions encountered) in the order they were supplied.
    - Returns an iterable of the results of the finished futures in the order they were supplied.
    """
    from async_anywhere.futures import AwaitableFuture

    if len(args) == 0:
        return ()

    futures = []
    for future in args:
        if isinstance(future, AwaitableFuture):
            future = future.as_async_future()
        elif isinstance(future, ConcurrentFuture):
            future = wrap_future(future)
        else:
            future = ensure_future(future)
        futures.append(future)
    assert len(futures) == len(set(futures))
    done, pending = await wait(futures, return_when=FIRST_COMPLETED, timeout=timeout)
    for p in pending:
        p.cancel("race(): not ready")
    results = []
    errors = []
    for fut in futures:
        if fut in done:
            done.discard(fut)
            fut.result()
            try:
                results.append(await fut)
            except Exception as e:
                errors.append(e)
    assert len(done) == 0
    raise_errors(*errors)
    return tuple(results)
