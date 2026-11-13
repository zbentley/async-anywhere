import logging
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
_NO_EXECUTOR.set_result(EventLoopExecutorStopped("SingletonEventLoopExecutor never started an EventLoopExecutor"))
P = ParamSpec("P")
T = TypeVar("T")


@final
class SingletonEventLoopExecutor(Executor):
    """A proxy for a single :py:class:`EventLoopExecutor` which can lazily construct it or replace it (when it is
    :ref:`stopped <stopping>`) on-demand.

    ``EventLoopExecutor``, unlike other ``concurrent.future.Executor`` subclasses, will not automatically restart or
    scale out its internal workers: it is intended to support use-cases where having at *most* a single executing thread
    is important (e.g. network clients to external systems that only support a single connection at a time). Because
    of that, when an ``EventLoopExecutor`` is :ref:`stopped <stopping>`, that is "forever": it cannot be started again.

    For some use cases, that constraint is inconvenient: if an ``EventLoopExecutor`` expires or is stopped due to an
    error, and then work needs to be done later on, it can be a hassle to ensure the executor is replaced and restarted:
    swapping out its instance in a globally-observable way is tricky, waiting for stopping executors to finish shutting
    down from synchronous or asynchronous code is tricky, preventing endless restart loops when executors cannot
    initialize is tricky, and handling those concerns everywhere an executor's futures can be waited on leads to
    cluttered code.

    To address those concerns, this class exists. It wraps a single :py:class:`EventLoopExecutor` instance, and
    automatically (re)starts it when :term:`jobs` are submitted, returning futures that wait for previous executors to
    shut down before running requested work on fresh instances.

    Example:

        >>> singleton = SingletonEventLoopExecutor(lazy=True)
        >>>
        >>> async def foo(): ...
        >>>
        >>> @singleton.bridge
        >>> async def bar(): ...
        >>>
        >>> # Run jobs on a lazily-constructed executor via .submit or @bridge:
        >>> singleton.submit(foo).result()
        >>> await singleton.submit(foo)
        >>> await bar()
        >>>
        >>> # Retrieve executor instances:
        >>> executor = singleton.get()
        >>> executor.shutdown()
        >>> # New executors are automatically re-created after waiting for old ones to shut down:
        >>> singleton.submit(foo).result()
        >>> # Shut down singleton executor until next job is requested:
        >>> await singleton.shutdown()

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

        # Validate main() now, since we'll be using it in .get() and lazy=True might not validate it inside the executor
        # constructor:
        main = kwargs.setdefault("main", wait_forever)
        if not iscoroutinefunction(main):
            raise TypeError(f"main must be a coroutine function (an async def), not {main!r}")
        self._main, self._kwargs = main, kwargs

        self._cancel_futures_on_stopping_executors = cancel_futures_on_stopping_executors
        self._wrapped_executor: ConditionProtected[None | EventLoopExecutor] = ConditionProtected(None)
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

        If a previously-constructed instance is not :ref:`stopped <stopping>`, it is returned.

        If a previously-constructed instance is stopped or stopping, it is signalled to shut down (optionally
        :ref:`cancelling <cancellation>` any outstanding :term:`jobs`; see the ``cancel_futures_on_stopping_executors``
        constructor parameter for details) and a new instance is constructed. The new instance will not run any jobs or
        its :term:`main task` until the old instance finishes shutting down."""
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
                    new._log(
                        logging.DEBUG,
                        "SingletonEventLoopExecutor: replacement executor %r waiting for previous executor %r to finish shutting down...",
                        new,
                        previous,
                    )
                    previous.join()  # Blocking its loop, so that calls made to it wait too: an await would let them run
                with stored as current:  # Once get() has stored it: which it never will if interrupted (by Ctrl+C, say)
                    ...
                if current is new and new.state is new.Running:  # Else it returns, stopping the executor
                    # Restarting its idle countdown, from run(), as this blocked: in the same step, so it can't fire first
                    new._expiry.reset(new._idle_timeout)
                    return await main(new)

            kwargs = {
                **self._kwargs,
                "_blocking_checker": self._blocking_checker,
                "main": gated,
            }
            return stored.replace_value(EventLoopExecutor(**kwargs))

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> AwaitableFuture[EventLoopExecutorStopped]:
        """
        Ensures the :py:class:`EventLoopExecutor` instance inside the invocant singleton is shut down.

        Args:
            wait: Synchronously blocks until the executor is shut down. See :py:meth:`EventLoopExecutor.shutdown`.
            cancel_futures: Calls cancel on all outstanding :term:`jobs` and the :term:`main task`.
                See :py:meth:`EventLoopExecutor.shutdown`.

        Raises:
            <Various Exceptions>: See :py:meth:`EventLoopExecutor.shutdown`.

        Returns:
            An AwaitableFuture whose result is a :py:class:`EventLoopExecutorStopped` instance containing details about
            the conditions under which the executor was stopped.

            If the :py:class:`EventLoopExecutor` instance inside the invocant singleton was not running or was never
            constructed, returns a completed AwaitableFuture.

            If ``wait=True``, returns a completed AwaitableFuture.

            Otherwise, returns an AwaitableFuture which will be completed once the singleton's executor instance has
            stopped.
        """
        executor = EventLoopExecutor.current()
        # If the current executor isn't the one managed by this instance, use the "with" result on _wrapped_executor to
        # set "executor" to the one we manage:
        if executor is None or executor._blocking_checker is not self._blocking_checker:
            with self._wrapped_executor as executor:
                pass
        if executor is None:
            return _NO_EXECUTOR
        return executor.shutdown(wait, cancel_futures=cancel_futures)

    def submit(
        self, fn: Callable[P, Coroutine[Any, Any, T]], /, *args: P.args, **kwargs: P.kwargs
    ) -> AwaitableFuture[T]:
        """
        Waits for the invocant's captive :py:class:`EventLoopExecutor` instance to be running and then calls
        :py:meth:`EventLoopExecutor.submit` to run a job on it.

        Args:
            fn:
            *args:
            **kwargs: See :py:meth:`EventLoopExecutor.submit`.

        Raises:
            <Various Exceptions>: See :py:meth:`EventLoopExecutor.submit`.

        Returns:
            An AwaitableFuture which will :term:`complete` when:
                1. Any stopping or broken executor previously stored on the invocant has finished shutting down *and*
                2. A running executor has been constructed, started, and stored on the invocant (see :py:meth:`get`) *and*
                3. ``fn`` has executed on the stored executor's captive thread and event loop via :py:meth:`EventLoopExecutor.submit`.
        """
        executor = self.get()
        try:
            return executor.submit(fn, *args, **kwargs)
        except EventLoopExecutorStopped:
            return self.get().submit(fn, *args, **kwargs)

    def bridge(self, func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, AwaitableFuture[T]]:
        """Wraps an async function and returns a wrapper which, when called, invokes the wrapped function on the
        invocant's executor via :py:meth:`submit`.

        See :py:meth:`EventLoopExecutor.bridge` for more details.
        """
        return update_wrapper(partial(self.submit, func), func)
