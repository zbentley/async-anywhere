"""An EventLoopExecutor that's replaced once it stops (by expiry, a crash, or a fork, say), for a client that should
outlive any one executor: a database connection, for one. Only one of its executors runs user code (its main, and calls)
at a time: a replacement is made at once, but runs nothing until the executor it replaces has finished."""
from functools import partial, update_wrapper
from inspect import iscoroutinefunction
from typing import Any, Callable, Coroutine, ParamSpec, TypeVar, final

from async_anywhere._util import BlockingChecker, LockProtected
from async_anywhere.exceptions import EventLoopExecutorStopped
from async_anywhere.executors import EventLoopExecutor, _wait_forever
from async_anywhere.executors._fork_safety import executors
from async_anywhere.futures import AwaitableFuture

# What shutdown() returns when there's no executor to stop: before the first get(), and in a forked process
_NO_EXECUTOR = AwaitableFuture()
_NO_EXECUTOR.set_result(None)
P = ParamSpec("P")
T = TypeVar("T")


@final
class SingletonEventLoopExecutor:
    def __init__(
        self,
        *,
        lazy: bool = True,
        cancel_futures_on_stopping_executors: bool = True,
        main: Callable[[EventLoopExecutor], Coroutine[Any, Any, Any]] = _wait_forever,
        **kwargs,
    ):
        """kwargs are EventLoopExecutor's, for each executor, as main is. With lazy=False, the first is made at once;
        otherwise, by the first get(). A stopping executor is shut down with
        shutdown(cancel_futures=cancel_futures_on_stopping_executors)."""
        if not iscoroutinefunction(main):  # Here, as the executors are given get()'s wrapper of it
            raise TypeError(f"main must be a coroutine function (an async def), not {main!r}")
        self._main, self._kwargs = main, kwargs
        self._cancel_futures_on_stopping_executors = cancel_futures_on_stopping_executors
        self._wrapped_executor = LockProtected(None)  # The executor, once get() has made one
        # Shared by its executors, each adding its own thread, so that their futures refuse blocking waits on any of them
        self._blocking_checker = BlockingChecker()
        executors.ensure_registered(self)
        if not lazy:
            self.get()

    def _after_fork_in_child(self):
        # A new lock, which a thread that isn't here may hold, and no executor, as its own has shut down here
        self._wrapped_executor = LockProtected(None)

    def get(self) -> EventLoopExecutor:
        """The executor. If it has stopped, a new one, which runs nothing (its main, or calls made to it) until the old one
        has shut down, and its thread finished: the old one is shut down, if it's stopping, but not waited for.
        Blocking waits for any of its executors' futures, on any of its executors' threads, raise WouldBlock: they could
        be waiting for a replacement that's waiting for them. Raises what EventLoopExecutor's constructor does."""
        with self._wrapped_executor as executor:
            if executor is not None:
                if executor.state is EventLoopExecutor.Running:
                    return executor
                executor.shutdown(wait=False, cancel_futures=self._cancel_futures_on_stopping_executors)
            executors.ensure_registered(self)  # Again, in a forked process, whose fork handler let go of it
            stored, main, previous = self._wrapped_executor, self._main, None if executor is None else executor._thread

            async def gated(new: EventLoopExecutor):  # Its main: run on its loop before anything else
                if previous is not None:
                    previous.join()  # Blocking its loop, so that calls made to it wait too: an await would let them run
                with stored as current:  # Once get() has stored it: which it never will if interrupted (by Ctrl+C, say)
                    begin = current is new
                if begin:  # Else it returns, stopping the executor
                    # Restarting its idle countdown, from run(), as this blocked: in the same step, so it can't fire first
                    new._expiry.reset(new._idle_timeout)
                    return await main(new)

            return stored.replace_value(EventLoopExecutor(
                main=gated,
                _blocking_checker=self._blocking_checker,
                **self._kwargs,
            ))

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> AwaitableFuture[EventLoopExecutorStopped | None]:
        """As EventLoopExecutor.shutdown(), for get()'s executor; or, called on one of its executors (one it has replaced,
        say), for that one, so that this never makes one. Each executor shuts down after those it replaced. Not for good:
        the next get() makes a new executor. With no executor to stop, it resolves with None at once."""
        executor = EventLoopExecutor.current()
        if executor is None or executor._blocking_checker is not self._blocking_checker:  # Not one of its executors
            with self._wrapped_executor as executor:
                pass
        if executor is None:
            return _NO_EXECUTOR
        return executor.shutdown(wait, cancel_futures=cancel_futures)

    def submit(
        self, fn: Callable[P, Coroutine[Any, Any, T]], /, *args: P.args, **kwargs: P.kwargs
    ) -> AwaitableFuture[T]:
        """As EventLoopExecutor.submit(), to get()'s executor; or, if that has stopped since get(), refusing the call, to
        its replacement, which get() makes once more."""
        executor = self.get()
        try:
            return executor.submit(fn, *args, **kwargs)
        except EventLoopExecutorStopped:
            return self.get().submit(fn, *args, **kwargs)

    def bridge(self, func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, AwaitableFuture[T]]:
        """As EventLoopExecutor.bridge(), its calls going through submit() above."""
        return update_wrapper(partial(self.submit, func), func)
