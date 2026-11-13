import asyncio
import logging
import threading
from collections import deque
from collections.abc import Callable, Collection
from functools import cache
from inspect import iscoroutinefunction
from threading import RLock, Thread, current_thread
from typing import TypeAlias, Union, final
from weakref import ref

from async_anywhere._util.fork_safety import ForkSafeInstanceTracker
from async_anywhere._util.trio import TrioRuntime
from async_anywhere.exceptions import WouldBlock


async def wait_forever(*_):
    await asyncio.Future()

def AnyEventLoop() -> bool:
    """Sentinel for the ``prevent_blocking_wait_in`` parameter of :py:meth:`AwaitableFuture.__init__`: raises
    :py:class:`.WouldBlock` instead of blocking if any event loop or :doc:`Trio <trio:index>` task is running."""
    return asyncio._get_running_loop() is not None or TrioRuntime.in_task()


def BlockingNotAllowed() -> bool:
    """Sentinel for the ``prevent_blocking_wait_in`` parameter of :py:meth:`AwaitableFuture.__init__`: raises
    :py:class:`.WouldBlock` instead of blocking in all cases."""
    return True

# TODO better annotate: incorporate constant sentinels.
# TODO better annotate: refer to it elsewhere.
# TODO better annotate: inline in docs.
BlockingEnvironments: TypeAlias = Collection[Union[asyncio.AbstractEventLoop, Thread, Callable[[], bool]]]

def validate_coroutine_function(_, attribute, value):
    if not iscoroutinefunction(value):
        raise TypeError(f"{attribute.name} must be a coroutine function (an async def), not {value!r}")


@cache
def futures_logger() -> logging.Logger:
    """
    What :py:class:`AwaitableFuture` logs to when un-observed errors or warnings occur (e.g. an :py:class:`Exception`
    is raised by a callback added via ``add_done_callback``--but not a :py:class:`BaseException`, those are raised
    and abort callback handling!).

    It's a child of the logger used internally by :py:class:`concurrent.futures.Future` for the same purpose, and is
    constructed and cached lazily. Laziness and child-ness allow user environments which customize the logging behavior
    of :py:class:`concurrent.futures.Future` to affect this logger as well.
    """
    import concurrent.futures._base
    # TODO guard against the symbol moving?
    return concurrent.futures._base.LOGGER.getChild("async_anywhere")

class Toggleable:
    # TODO perf of having a __bool__ *property*?
    __slots__ = ("_value",)

    def __init__(self):
        self._value = False

    def set(self):
        self._value = True

    def __bool__(self):
        return self._value

@final
class ConditionProtected[T](threading.Condition):
    """A threading.Condition over an RLock, which also guards a value, which entering it gives. Never left held by a signal
    handler's exception (see __enter__()), and reset in each child process os.fork()ed from one that has it, unless the
    forking thread held it (see _at_fork_reinit())."""
    # __exit__ is a slot, not a method: with-statements look it up on the class, whose slot descriptor gives them this
    # instance's lock's own (C) __exit__, so leaving a with-block runs no Python frame
    __slots__ = ("_value", "__exit__")
    __exit__: Callable[..., None]

    def __init__(self, value: T):
        # threading.Condition.__init__()'s fields, set without calling it, or its checks of what an RLock has: this is
        # made with each StateTracker. An RLock, not a Lock, which neither __enter__() nor _at_fork_reinit() can handle.
        # Of the lock's methods, copied are just those each completed future uses, and _acquire_restore(): a Python
        # forwarder's entry runs a check, where a KeyboardInterrupt would leave wait() without the lock back. The rest are
        # forwarded below, saving each construction their bound methods.
        self._lock = lock = RLock()
        self.acquire = lock.acquire
        self._is_owned = lock._is_owned
        self._acquire_restore = lock._acquire_restore
        self.__exit__ = lock.__exit__
        self._waiters = deque()
        self._value = value
        _conditions.ensure_registered(self)

    def __enter__(self) -> T:
        """
        Acquires the lock and returns the value, making sure the lock is released if anything goes wrong during
        acquisition.

        All RLocks and things based on them are, in CPython, interrupt (as in KeyboardInterrupt/signals) unsafe: they
        can, if interrupted while being acquired, be left held. This sucks a lot with regards to context managers,
        because __enter__ can raise before the manager is entered (meaning that __exit__ isn't run) but after the
        manager was mutated (lock was acquired), leaving things in a super deadlock-prone state.

        This problem can't be solved; interrupt-unsafety is a feature of basically all Python code that doesn't manage
        signal delivery directly: https://docs.python.org/3/library/signal.html#note-on-signal-handlers-and-exceptions

        Several things make this worse nowadays for this module's code:
        - Python 3.14 made the window during which context-manager-entry corruption can occur with a stdlib lock wider;
          https://github.com/python/cpython/issues/74174.
        - Lots of code in this module is coordinated with locks and designed to be used from synchronous main-thread
          Python, where signals get delivered by default.
        - We heavily use RLocks and Python context manager wrappers around locks, widening the window further.
        - EventLoopExecutor futures being waited on in the main thread are an area where CTRL+C is likely to hit a lot.
          If it hits during a lock acquire, the executor can get stuck inside its worker thread trying to interact with
          the job. Then, anyone using the executor itself as a contextmanager will hit a deadlock-wait with executor
          shutdown on exit, requiring CTRL+C spamming to shut the process down.

        This method slightly mitigates those risk factors. NOTE, again, that there are plenty of places in this module
        where CTRL+C will cause a hang. This method's "release on BaseException during context manager entry" just
        shaves off a few digits from the probability of it happening in one common case. This module provides no general
        facilities for interrupt-safe Python code. If you want that, run your whole program inside an event loop, move
        signal-sensitive code onto a thread, or use Trio.
        """
        try:
            self.acquire()
        except BaseException:
            try:
                # The lock's own (C) release(), not this class's: entering a Python one runs a check before letting go
                self._lock.release()  # Taken, unless acquire() was interrupted waiting: an RLock this thread holds never waits
            except RuntimeError:  # Not taken
                pass
            raise
        return self._value

    def release(self):
        self._lock.release()

    def locked(self) -> bool:
        return self._lock.locked()

    def _release_save(self):  # threading.Condition's own is for a plain Lock, which has no recursion level to save
        return self._lock._release_save()

    def replace_value(self, value: T) -> T:
        """Replaces the value. Not thread-safe unless called inside this class's context manager."""
        self._value = value
        return value

    def _at_fork_reinit(self):
        # As threading.Condition's, but leaving the lock alone if the forking thread holds it, as that thread lets go
        self._waiters.clear()  # Their threads aren't here
        if not self._is_owned():
            self._lock._at_fork_reinit()


class _ConditionForkSafety(ForkSafeInstanceTracker):
    def after_fork_in_child(self):
        for condition in self:  # A WeakSet iterates a copy of itself
            condition._at_fork_reinit()


# Every ConditionProtected. Made, so its fork handler registered, as this module is imported: before this package's
# others (in its _fork_safety modules), which so find every condition reset
_conditions = _ConditionForkSafety()


@final
class BlockingChecker:
    """Where blocking waits for a future would deadlock, so are refused: on given threads, in given event loops (both held
    weakly), in any running event loop or trio task (AnyEventLoop), or anywhere (BlockingNotAllowed)."""
    __slots__ = ("_environments",)

    def __init__(self, environments: BlockingEnvironments = ()):
        # TODO check for perf improvements to be had by combining init and weakened and re-calling init elsewhere?
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

    def copy(self, *, add: BlockingEnvironments = ()) -> BlockingChecker:
        """A new checker, refusing what this one does now, and `environments` too."""
        new = BlockingChecker.__new__(BlockingChecker)
        new._environments = self._environments + BlockingChecker._weakened(add)
        return new

    def add(self, environment: asyncio.AbstractEventLoop | Thread | Callable[[], bool]):
        # Dropping what's gone, as a SingletonEventLoopExecutor's checker gets a thread for every executor it makes
        kept = [env for env in self._environments if not isinstance(env, ref) or env() is not None]
        self._environments = (*kept, *BlockingChecker._weakened((environment,)))

    def check(self, subject):
        thread, loop, culprit = current_thread(), asyncio._get_running_loop(), None
        try:
            for environment in self._environments:
                if isinstance(environment, ref):  # A thread or loop, or None once it's gone
                    if (culprit := environment()) is thread or (loop is not None and culprit is loop):
                        raise WouldBlock(subject, culprit)
                elif environment():  # AnyEventLoop or BlockingNotAllowed
                    raise WouldBlock(subject, environment)
        finally:
            del subject, thread, loop, culprit  # Else what this raises would keep them alive, through its traceback
