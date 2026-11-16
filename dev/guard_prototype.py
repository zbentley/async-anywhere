"""Prototype: faster lock/nullcontext-swappable wrapper, since context managers are slow

    class Counter:
        __slots__ = ("_guard", "count")

        def __init__(self, thread_safe=True):
            self.count = 0
            self._guard = Guard(self, RLock() if thread_safe else None, (Counter._bump,))

        @locked
        def _bump(self):
            self.count += 1
            return self.count

        def bump(self):
            return self._guard._bump()
"""
from collections.abc import Callable
from threading import RLock
from types import MethodType


def _run(state):  # Module-level: under free threading, a closure shared by every guard contends on each call
    function, value, lock = state
    try:
        lock.acquire()  # Inside the try: a KeyboardInterrupt raised as it returns still reaches the release
        return function(value)
    finally:
        try:
            lock.release()
        except RuntimeError:  # acquire() was interrupted waiting, so never took the lock
            pass


class locked:
    __slots__ = ("function",)

    def __init__(self, function: Callable[[object], object]):
        if hasattr(Guard, function.__name__):  # Guards store its callable under its name
            raise ValueError(f"Guards can't hold a function named {function.__name__!r}")
        self.function = function


class Guard:

    __slots__ = ("_value", "_lock", "_bound", "__dict__")  # Callables go in __dict__, under names locked() vetted

    def __init__(self, value, lock: RLock | None = None, functions: tuple[locked, ...] = ()):
        self._value = value
        self._lock = lock
        self._bound = functions
        for function in functions:  # Not via bind(), whose checks cost more than the binding: made per owner
            function = function.function
            setattr(self, function.__name__, MethodType(function, value) if lock is None else MethodType(_run, (function, value, lock)))

    def bind(self, function: locked | Callable[[object], object]) -> locked:
        if not isinstance(function, locked):
            function = locked(function)
        name = function.function.__name__
        if hasattr(self, name):  # Not self.__dict__, whose first use would make a real dict of its inline values
            raise ValueError(f"{self!r} already has {name!r}")
        self._bound += (function,)
        lock = self._lock
        setattr(self, name, MethodType(function.function, self._value) if lock is None else MethodType(_run, (function.function, self._value, lock)))
        return function

    def unlock(self):
        self._lock = None  # Calls already running hold their own, in their _run() state
        for function in self._bound:
            setattr(self, function.function.__name__, MethodType(function.function, self._value))
