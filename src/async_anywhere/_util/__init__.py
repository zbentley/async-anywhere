import asyncio
import logging
import threading
from collections.abc import Callable, Collection
from functools import cache
from inspect import iscoroutinefunction
from threading import RLock, Thread, current_thread
from typing import TypeAlias, Union, final
from weakref import ref

from async_anywhere._util.trio import TrioRuntime
from async_anywhere.exceptions import WouldBlock

def AnyEventLoop() -> bool:
    """Sentinel for the ``prevent_blocking_wait_when`` parameter of :py:meth:`AwaitableFuture.__init__`: raises
    :py:class:`.WouldBlock` instead of blocking if any event loop or :doc:`Trio <trio:index>` task is running."""
    return asyncio._get_running_loop() is not None or TrioRuntime.in_task()


def BlockingNotAllowed() -> bool:
    """Sentinel for the ``prevent_blocking_wait_when`` parameter of :py:meth:`AwaitableFuture.__init__`: raises
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

def enter(protected: Condition | LockProtected):
    """The ``__enter__`` of Condition and LockProtected: acquires their RLock and returns their ``_value``, letting go of
    the lock again if anything raises first, so that no ``with`` block on them can leave it held.

    The problem: CPython runs Python-level signal handlers (such as the default SIGINT handler, which raises
    KeyboardInterrupt for Ctrl+C) at the interpreter's periodic checks between bytecodes, and one of those is as a call to
    a C function returns. So a handler can raise just as a lock's ``acquire()``, or a C lock's ``__enter__``, returns,
    having taken the lock, but before any ``with`` or ``try`` covers the hold. The exception propagates, ``__exit__``
    never runs, and the next thread to take the lock deadlocks. CPython's docs say most code, the standard library's
    included, can't be made robust against this: see `Note on Signal Handlers and Exceptions
    <https://docs.python.org/3/library/signal.html#note-on-signal-handlers-and-exceptions>`_.

    - Python-level ``__enter__`` methods have always had this gap. ``threading.Condition``'s is one, and so
      ``threading.Event``, ``threading.Semaphore``, ``queue.Queue`` and ``concurrent.futures.Future`` all have it.
    - ``with`` on a C lock had it too, until `gh-74174 <https://github.com/python/cpython/issues/74174>`_ was fixed in
      2021: from 3.11 to 3.13, `BEFORE_WITH <https://docs.python.org/3.13/library/dis.html#opcode-BEFORE_WITH>`_
      entered the lock with no check in between. 3.14 brings the gap back. It compiles ``with`` to
      `LOAD_SPECIAL <https://docs.python.org/3.14/library/dis.html#opcode-LOAD_SPECIAL>`_ and an ordinary call to
      ``__enter__``, and the block's protection begins only after that call returns, and its check runs.

    The fix: returning from a call to a Python function runs no check (entering one does), so once this returns, the
    ``with`` block's protection begins with no gap. Before then, the ``try`` below covers ``acquire()``'s return: if
    anything raises once the lock has been taken, this lets go before raising it again.

    - This needs an RLock: ``release()`` then succeeds just when this thread holds it. If ``acquire()`` was interrupted
      waiting for another thread, ``release()`` raises RuntimeError, as nothing was taken. An RLock this thread already
      holds never waits, so a reentrant ``acquire()`` can only be interrupted once it has taken the lock. A plain Lock
      has no owner, so it can't tell the two cases apart.
    - What lets go must be C as well. Entering a Python ``__exit__`` runs a check before it could let go, which is why
      both classes keep their lock's own ``__exit__`` in a slot.
    - Not covered: an interrupt as ``threading.Condition.wait()`` lets go of the lock, before its own ``try`` begins.
      Nothing is left held then, but ``__exit__`` raises RuntimeError ("cannot release un-acquired lock"), with the
      KeyboardInterrupt as its context, in place of the KeyboardInterrupt. Nor are stdlib locks this package doesn't
      make, such as logging handlers'.

    Why this package is more exposed than most code:

    - An EventLoopExecutor's thread keeps running after its caller's KeyboardInterrupt, and takes the same locks as its
      callers. These include each job future's Condition, which the caller's thread takes in ``result()``,
      ``exception()``, ``cancel()`` and ``add_done_callback()``, and the loop's thread takes to complete the future.
      They also include the executor's own ``_condition``, which ``submit()`` and ``shutdown()`` take on the caller's
      thread, and the end of each job on the loop's. One left held by the caller's thread wedges the loop's thread for
      good, so no job or state future resolves again.
    - The usual ways out then wait for that thread: ``with executor:`` exits through ``shutdown(wait=True)``, and callers
      block in ``result()``. So a lock that would merely leak as a plain program exits becomes a hang, which only a
      second Ctrl+C breaks.
    - Synchronous callers get KeyboardInterrupt raised wherever they are. Callers under ``asyncio.run()`` mostly don't:
      its first Ctrl+C cancels the main task rather than raising (see `Handling Keyboard Interruption
      <https://docs.python.org/3/library/asyncio-runner.html#handling-keyboard-interruption>`_). Trio defers it only in
      its own code (see `Safer KeyboardInterrupt handling
      <https://trio.readthedocs.io/en/stable/reference-lowlevel.html#safer-keyboardinterrupt-handling>`_), not in this
      package's.
    - This package requires Python 3.14, where even ``with`` on a C lock has the gap.
    """
    try:
        protected.acquire()
    except BaseException:
        try:
            protected.release()  # Taken, unless acquire() was interrupted waiting: an RLock this thread holds never waits
        except RuntimeError:  # Not taken
            pass
        raise
    return protected._value


@final
class Condition(threading.Condition):
    """A threading.Condition over an RLock, never left held by a signal handler's exception: see enter()."""
    __slots__ = ("__exit__",)  # As LockProtected's: its lock's own (C) __exit__
    _value = True  # What enter() returns, as threading.Condition.__enter__() does

    def __init__(self):
        super().__init__(RLock())  # Not a Lock, which enter() can't handle
        self.__exit__ = self._lock.__exit__

    __enter__ = enter


@final
class LockProtected[T]:
    # __exit__ is a slot, not a method: with-statements look it up on the class, whose slot descriptor gives them this
    # instance's lock's own (C) __exit__, so leaving a with-block runs no Python frame
    __slots__ = ("acquire", "release", "_value", "__exit__")  # Its RLock's own methods, bar _value: see enter()
    __exit__: Callable[..., None]

    def __init__(self, value: T):
        lock = RLock()  # Not a Lock, which enter() can't handle
        self.acquire, self.release, self._value, self.__exit__ = lock.acquire, lock.release, value, lock.__exit__

    __enter__ = enter

    def replace_value(self, value: T) -> T:
        """Replaces the value. Not thread-safe unless called inside this class's context manager."""
        self._value = value
        return value

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
