from asyncio import Future, AbstractEventLoop, run_coroutine_threadsafe
from asyncio import wrap_future, wait, FIRST_COMPLETED, ensure_future, get_running_loop, gather as asyncio_gather
from concurrent.futures import Future as ConcurrentFuture
from contextlib import AbstractContextManager, suppress
from functools import partial
from inspect import iscoroutinefunction
from sys import exc_info
from threading import RLock, Lock
from typing import Awaitable, AsyncGenerator, Self, Callable

from async_anywhere.futures import AwaitableFuture


def running_loop() -> AbstractEventLoop | None:
    try:
        return get_running_loop()
    except RuntimeError:
        return None


def run_on_loop[_T, **_P](
        loop: Future | AbstractEventLoop,
        delay: float | None,
        func: Callable[_P, _T],
        *args: _P.args,
        **kwargs: _P.kwargs,
):
    """Given a loop source (a loop, asyncio.Future, or asyncio.Task), a delay, a function, and arguments, arranges
    for the function to be called "on" the supplied loop (in its execution/thread context).

    To distinguish exceptions raised by the async runtime (e.g. dead-loop, invalid-timeout) from exceptions raised by
    user code, consider handling the result of this method separately from the result of its call. For an async "func",
    that would look like "try: f = run_on_loop(...); except: ...; await f". For a synchronous call, consider using
    functools.partial as "func".

    Note that unless "delay" is None, this function does NOT produce an awaitable whose result is the return value of
    the supplied function (TODO, that might be convenient).
    """
    if not isinstance(loop, AbstractEventLoop):
        loop = loop.get_loop()
    loop: AbstractEventLoop

    func = partial(func, *args, **kwargs)
    if loop is running_loop():
        if delay is None:
            return func()
        else:
            return loop.call_later(delay, func)
    elif delay is None:
        if iscoroutinefunction(func):
            return run_coroutine_threadsafe(func(), loop)
        else:
            return loop.call_soon_threadsafe(func)
    else:
        # Have the loop start a timer, thread-safely, to call our code in the future.
        return loop.call_soon_threadsafe(run_on_loop, loop, None, loop.call_later, delay, func)




class LockProtected[T](AbstractContextManager[T]):
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
