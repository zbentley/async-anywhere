import asyncio
import logging
from collections.abc import Callable, Collection
from functools import cache
from inspect import iscoroutinefunction
from threading import Lock, RLock, Thread, current_thread
from typing import final
from weakref import ref

from async_anywhere._util.trio import TrioRuntime
from async_anywhere.exceptions import WouldBlock

type BlockingEnvironments = Collection[asyncio.AbstractEventLoop | Thread | Callable[[], bool]]

def validate_coroutine_function(_, attribute, value):
    if not iscoroutinefunction(value):
        raise TypeError(f"{attribute.name} must be a coroutine function (an async def), not {value!r}")


@cache
def futures_logger() -> logging.Logger:
    """
    What :py:mod:`AwaitableFuture` logs to when un-observed errors or warnings occur (e.g. an :py:class:`Exception`
    is raised by a callback added via ``add_done_callback``--but not a :py:class:`BaseException`, those are raised
    and abort callback handling!).

    It's a child of the logger used internally by :py:class:`concurrent.futures.Future` for the same purpose, and is
    constructed and cached lazily. Laziness and child-ness allow user environments which customize the logging behavior
    of :py:class:`concurrent.futures.Future` to affect this logger as well.
    """
    import concurrent.futures._base
    # TODO guard against the symbol moving?
    return concurrent.futures._base.LOGGER.getChild("async_anywhere")


@final
class LockProtected[T]:
    """A value reachable only under its lock: `with protected as value:` holds the lock while the block runs."""
    __slots__ = ("_acquire", "_release", "_value")

    def __init__(self, value: T, recursive: bool = False):
        lock = RLock() if recursive else Lock()
        # Bound once: calling these is cheaper than looking up the lock's methods per call
        self._acquire, self._release = lock.acquire, lock.release
        self._value = value

    def __enter__(self) -> T:
        self._acquire()
        return self._value

    def __exit__(self, exc_type, exc, tb):
        self._release()

    def replace_value(self, value: T) -> T:
        """Replaces the values. Not thread-safe unless called inside this class's context manager."""
        self._value = value
        return value


def AnyEventLoop() -> bool:
    """Sentinel for :py:obj:`AwaitableFuture.prevent_blocking_wait_when`: raises :py:class:`WouldBlock` instead of
    blocking if any event loop or Trio task is running."""
    return asyncio._get_running_loop() is not None or TrioRuntime.in_task()


def BlockingNotAllowed() -> bool:
    """Sentinel for :py:obj:`AwaitableFuture.prevent_blocking_wait_when`: raises :py:class:`WouldBlock` instead of
    blocking in all cases."""
    return True


@final
class BlockingChecker:
    """Where blocking waits for a future would deadlock, so are refused: on given threads, in given event loops (both held
    weakly), in any running event loop or trio task (AnyEventLoop), or anywhere (BlockingNotAllowed)."""
    __slots__ = ("_environments",)

    def __init__(self, environments: BlockingEnvironments = ()):
        self._environments = BlockingChecker._weakened(environments)

    @staticmethod
    def _weakened(environments: Collection) -> tuple:
        # Threads and loops as weak references, so that futures never keep them alive: AnyEventLoop and
        # BlockingNotAllowed as they are
        weakened = []
        for environment in environments:
            if isinstance(environment, (asyncio.AbstractEventLoop, Thread)):
                weakened.append(ref(environment))
            elif environment is AnyEventLoop or environment is BlockingNotAllowed:
                weakened.append(environment)
            else:
                raise TypeError(f"Blocking waits can be refused in event loops, on threads, or by AnyEventLoop or BlockingNotAllowed, not {environment!r}")
        return tuple(weakened)

    def copy(self, environments: BlockingEnvironments = ()) -> BlockingChecker:
        """A new checker, refusing what this one does now, and `environments` too."""
        new = BlockingChecker.__new__(BlockingChecker)
        new._environments = self._environments + BlockingChecker._weakened(environments)
        return new

    def add(self, environment: asyncio.AbstractEventLoop | Thread | Callable[[], bool]):
        """Refuses blocking waits in `environment` too, wherever this checker is used (but not in its copies). Concurrent
        add()s must be serialized; check()s needn't be, as each sees the environments from before an add(), or after."""
        # Dropping what's gone, as a SingletonEventLoopExecutor's checker gets a thread for every worker it makes
        kept = [env for env in self._environments if not isinstance(env, ref) or env() is not None]
        self._environments = (*kept, *BlockingChecker._weakened((environment,)))

    def check(self, future):
        """Raises WouldBlock, naming `future` and what refused the wait, if a blocking wait for it here is refused."""
        thread, loop, culprit = current_thread(), asyncio._get_running_loop(), None
        try:
            for environment in self._environments:
                if isinstance(environment, ref):  # A thread or loop, or None once it's gone
                    if (culprit := environment()) is thread or (loop is not None and culprit is loop):
                        raise WouldBlock(future, culprit)
                elif environment():  # AnyEventLoop or BlockingNotAllowed
                    raise WouldBlock(future, environment)
        finally:
            del future, thread, loop, culprit  # Else what this raises would keep them alive, through its traceback
