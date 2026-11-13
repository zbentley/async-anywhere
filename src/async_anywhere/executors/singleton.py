"""An EventLoopExecutor that's replaced once it stops (by expiry, a crash, or a fork, say), for a client that should
outlive any one executor: a database connection, for one. Only one of its executors runs user code (its :term:`main task`, and :term:`jobs <job>`)
at a time: a replacement is made at once, but runs nothing until the executor it replaces has finished."""
from functools import partial, update_wrapper
from inspect import iscoroutinefunction
from typing import Any, Callable, Coroutine, ParamSpec, TypeVar, final
from concurrent.futures import Executor
from async_anywhere._util import BlockingChecker, ConditionProtected, wait_forever
from async_anywhere.exceptions import EventLoopExecutorStopped
from async_anywhere.executors import EventLoopExecutor
from async_anywhere.executors._fork_safety import executors
from async_anywhere.futures import AwaitableFuture

# What shutdown() returns when there's no executor to stop: before the first get(), and in a forked process
_NO_EXECUTOR = AwaitableFuture()
_NO_EXECUTOR.set_result(None)
P = ParamSpec("P")
T = TypeVar("T")


@final
class SingletonEventLoopExecutor(Executor):
    """A proxy for a single :py:class:`EventLoopExecutor` which can lazily construct it or replace it (when it is
    :ref:`stopped`) on-demand.

    ``EventLoopExecutor``, unlike other ``concurrent.future.Executor`` subclasses, will not automatically restart or
    scale out its internal workers: it is intended to support use-cases where having at *most* a single executing thread
    is important (e.g. network clients to external systems that only support a single connection at a time). Because
    of that, when an ``EventLoopExecutor`` is :ref:`stopped`, that is "forever": it cannot be started again.

    For some use cases, that constraint is inconvenient: if an ``EventLoopExecutor`` expires or is stopped due to an
    error, and then work needs to be done later on, it can be a hassle to ensure the executor is replaced and restarted:
    swapping out its instance in a globally-observable way is tricky, waiting for stopping executors to finish shutting
    down from synchronous or asynchronous code is tricky, preventing endless restart loops when executors cannot
    initialize is tricky, and handling those concerns everywhere an executor's futures can be waited on leads to
    cluttered code.

    To address those concerns, this class exists. It wraps a single :py:class:`EventLoopExecutor` instance, and
    automatically (re)starts it when :term:`jobs` are submitted, returning futures that wait for previous executors to
    shut down before running requested work on fresh instances.

    """
    def __init__(
        self,
        *,
        lazy: bool = True,
        cancel_futures_on_stopping_executors: bool = True,
        **kwargs,
    ):
        """
        Construct a singleton proxy for one :py:class:`EventLoopExecutor`, optionally also constructing and starting
        the thread/event loop of the wrapped executor.

        Args:
            lazy: If True, the executor will be constructed and started on first use (e.g. when :py:meth:`get` or
                :py:meth:`submit` is called). If False, it will be constructed and started before this constructor
                returns.
            cancel_futures_on_stopping_executors: If True (the default), whenever users request an executor from this
                class (e.g. when :py:meth:`get` or :py:meth:`submit` is called) and the stored executor is stopping,
                all outstanding :term:`jobs` on the stopping executor will be cancelled (as if
                ``EventLoopExecutor.shutdown(cancel_futures=True)`` were called) before this class waits for that
                executor to stop, constructs and starts a new one, and then forwards the user's request on to it.
            **kwargs: Keyword arguments to forward to the constructor of :py:class:`EventLoopExecutor` to construct the
                executor instances used by this class.
        """
        super().__init__()  # In case stdlib ever adds a constructor (probably won't)
        main = kwargs.setdefault("main", wait_forever)
        if not iscoroutinefunction(main):  # Here, as the executors are given get()'s wrapper of it
            raise TypeError(f"main must be a coroutine function (an async def), not {main!r}")
        self._main, self._kwargs = main, kwargs
        self._cancel_futures_on_stopping_executors = cancel_futures_on_stopping_executors
        self._wrapped_executor = ConditionProtected(None)  # The executor, once get() has made one
        # Shared by its executors, each adding its own thread, so that their futures refuse blocking waits on any of them
        self._blocking_checker = BlockingChecker()
        executors.ensure_registered(self)
        if not lazy:
            self.get()

    def _after_fork_in_child(self):
        # No executor, as its own has shut down here. Its lock, which a thread that isn't here may hold, resets itself
        self._wrapped_executor.replace_value(None)

    def get(self) -> EventLoopExecutor:
        """Retrieves the stored :py:class:`EventLoopExecutor` instance.

        If no instance has been created due to ``lazy=True``, an instance is constructed now.

        If a previously-constructed instance is not :ref:`stopped`, it is returned.

        If a previously-constructed instance is stopped or stopping, it is signalled to shut down (optionally cancelling
        any outstanding :term:`jobs`; see the ``cancel_futures_on_stopping_executors`` constructor parameter for
        details) and a new instance is constructed. The new instance will not run any jobs or its :term:`main task`
        until the old instance finishes shutting down."""
        # fut = AwaitableFuture(cancel_pending_in_forked_child=True)
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

            kwargs = {
                **self._kwargs,
                "_blocking_checker": self._blocking_checker,
                "main": gated,
            }
            return stored.replace_value(EventLoopExecutor(**kwargs))

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
        """As EventLoopExecutor.submit(), to get()'s executor; or, if that has stopped since get(), refusing the job, to
        its replacement, which get() makes once more."""
        executor = self.get()
        try:
            return executor.submit(fn, *args, **kwargs)
        except EventLoopExecutorStopped:
            return self.get().submit(fn, *args, **kwargs)

    def bridge(self, func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, AwaitableFuture[T]]:
        """As EventLoopExecutor.bridge(), its jobs going through submit() above."""
        return update_wrapper(partial(self.submit, func), func)
