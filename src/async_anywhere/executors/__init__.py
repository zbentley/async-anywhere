import asyncio
import concurrent.futures
import logging
from contextlib import suppress
from contextvars import Context, copy_context
from datetime import timedelta
from functools import cached_property, partial, update_wrapper
from threading import Thread, current_thread, main_thread, RLock
from typing import Any, Callable, ClassVar, Coroutine, Literal, ParamSpec, TypeVar, final
from weakref import WeakSet

import sniffio
from attrs import Factory, define, field, setters, validators

from async_anywhere._util import LoopSelector, blocking_wait_checker, validate_coroutine_function, copy_exception
from async_anywhere.executors import _fork_safety
from async_anywhere.executors.exceptions import CleanStop, WorkerBroken, WorkerInterrupted, WorkerStopped, WorkerForked
from async_anywhere.executors._timer import Timer
from async_anywhere.futures import AwaitableFuture
from async_anywhere.state_tracker import State
from async_anywhere.state_tracker.waitable import WaitableStateTracker


# Not PEP 695 type params, which Sphinx mistakes for typing's own TypeVars and drops from docs. TODO: file Sphinx bug
_P = ParamSpec("_P")
_T = TypeVar("_T")


async def _wait_forever(_):
    await asyncio.Future()

def _seconds(timeout: float | timedelta) -> float:
    return timeout.total_seconds() if isinstance(timeout, timedelta) else timeout

def _timer_factory(worker: EventLoopExecutor):
    return Timer(
        loop=worker._loop,
        callback=lambda: worker._clean_stop("expired") and worker._interrupt(True),
    )

@final
@define(
    kw_only=True,
    eq=False, # Make equality and hashing use object identity alone.
    on_setattr=setters.frozen, # Immutable fields. Not frozen=True, which would freeze the StateTracker's state too.
    repr=False, # Repr gets extremely verbose and not useful.
    auto_attribs=False,
)
class EventLoopExecutor(concurrent.futures.Executor, WaitableStateTracker):
    """
    An executor which contains a single thread running an event loop, and which can run asynchronous code on that loop.

    The captive thread is started when an EventLoopExecutor is constructed, and is never replaced.

    If you need an executor which starts lazily and/or restarts its thread after stop, see `SingletonEventLoopExecutor`(TODO).

    Once a worker is stopped, it will not accept new jobs. In-progress jobs and the "main" task may be interrupted, or
    may continue running, depending on how a stop was triggered. See `Stopping`(TODO) for more information.

    Usage:
        Jobs can be submitted to an executor via `.submit()` or via the `@bridge` decorator:

        >>> executor = EventLoopExecutor()
        >>>
        >>> async def foo(arg, kwarg=None): ...
        >>>
        >>> @executor.bridge
        >>> async def bar(arg, kwarg=None): ...
        >>>
        >>> executor.submit(foo, "arg", kwarg=True)  # Runs foo() on the captive loop and returns an AwaitableFuture
        >>> bar("arg", kwarg=True) # Runs bar() on the captive loop and returns an AwaitableFuture
        >>> executor.map(foo, [[1], [2]], timeout=1)  # Works as a concurrent.futures.Executor

        The return value of `.submit()` and `@bridge`d functions is an `AwaitableFuture`, which can be awaited on, or
        blocked on, from any number of event loops or threads.

        Code running inside an EventLoopExecutor can itself run jobs on the executor.

    Advanced usage:
        EventLoopExecutors have several optional features for advanced use:

        - They can time out and shut themselves down after a configurable idle time.
        - Users can supply a custom "main" awaitable function which will always be running as an asyncio.Task inside the
          EventLoopExecutor. If that awaitable ever completes, the executor will shut itself down.
        - Users can supply custom event loops to be used inside of the executor. This facilitates e.g. combining uvloop
          and other event loops within the same program.
        - Users can supply a custom logger that the EventLoopExecutor will use to emit lifecycle events.

        >>> async def connect_and_run_forever(executor): ...
        >>>
        >>> executor = EventLoopExecutor(
        >>>    main=connect_and_run_forever,
        >>>    idle_timeout=timedelta(minutes=5),
        >>>    loop_factory=uvloop.new_event_loop,
        >>>    logger=logging.getLogger("mylogger"),
        >>> )

    Note:

        Every method in an Executor's public API can be called (and its result waited for, if it returns a future) from
        `any` thread or event loop context within the process that created the EventLoopExecutor it is bound to.
        This includes things like calling ``.submit()`` from the executor's own loop and thread.

    Trio:
        ``await``ing on the result of ``submit()`` or ``@bridge`` from Trio works automatically. Running code in an
        executor that uses Trio requires some setup, since the internal machinery of EventLoopExecutor is asyncio-only
        at this time. To run Trio code in an executor, use Trio's facilities for acting as a "guest" of another event
        loop, e.g. ``trio.lowlevel.start_guest_run()``. See tests/test_executors/test_runtimes.py for examples.

    """


    # TODO: its validator's errors name _parent_task, not its "main" alias
    _parent_task: Callable[[EventLoopExecutor], Coroutine[Any, Any, Any]] = field(
        alias="main",
        default=_wait_forever,
        validator=validate_coroutine_function
    )
    """A coroutine function (``async def``) that will be started inside the executor during construction. When this
    function completes, the executor will stop itself via ``shutdown(wait=False)``; otherwise, "main" is treated like any
    other long-running job submitted to the executor. Other jobs run on the executor will run concurrently with this
    function.
    
    The supplied callable will be invoked only from within a running event loop, and must take a single parameter: the
    executor itself.
    
    If no value is supplied, an executor will wait forever until stopped.
    """
    # TODO: its validators' errors name _idle_timeout, not its "idle_timeout" alias
    _idle_timeout: float = field(
        default=float("inf"),
        converter=_seconds,  # Before the validators, so a timedelta's checked as its total_seconds()
        validator=[validators.instance_of((int, float)), validators.gt(0)],
    )
    """An amount of time spent idle, after which the executor will stop itself via ``shutdown(wait=False)``.
    The time-spend-idle tracker resets every time a job is submitted to an executor. An executor will never expire while
    a job is running. Must be a positive ``datetime.timedelta`` or floating-point number of seconds. 
    
    Defaults to ``float("inf")``, i.e. never time out.
    """
    # Makes the worker's loop, once, as it's constructed: see _loop
    _loop_factory: Callable[[], asyncio.AbstractEventLoop] = field(
        default=asyncio.new_event_loop, validator=validators.is_callable()
    )
    """A callable which creates a new event loop that the executor will use in its captive thread. This callable must
    not return a reference to a loop that is in use anywhere else, and must not return a loop that is in any way bound
    to the current thread, as the executor may construct the loop in a different thread than the one that runs it.
    
    Defaults to ``asyncio.new_event_loop``.
    """
    logger: logging.Logger = field(
        factory=lambda: logging.getLogger(EventLoopExecutor.__qualname__),
    )
    """A logger that the executor will use to log lifecycle events (certain failures and stop conditions). The executor 
    will never use this logger across process boundaries in the event of a fork(2).
    
    Defaults to a new logger named after the executor.
    
    Note:
        Using this logger must not unduly block the event loop. Loggers that wait on network services or slow disks can
        cause the executor to hang.
    """

    # Bridged calls in flight: their jobs' tasks. They hold off idle expiry, and drains wait for them; see _interrupt().
    _calls: set[asyncio.Task] = field(init=False, factory=set)
    # Whether the worker has cancelled its parent task, calls, or what they left running
    _interrupted: bool = field(init=False, default=False, on_setattr=setters.NO_OP)
    # The parent task's task, once it has started, which the constructor waits for: see _main()
    _runner: AwaitableFuture[asyncio.Task] = field(init=False, factory=AwaitableFuture)
    _expiry: Timer = field(init=False, default=Factory(_timer_factory, takes_self=True))
    # Calls' futures, which _finalize() fails if the worker won't finish them (if the loop drops them unrun as it closes,
    # say). Weak, as a call that's done, and dropped by its caller, needs nothing more. Changed only under _condition,
    # which transitions take too, so none is added once the worker has stopped.
    _submitted: WeakSet[AwaitableFuture] = field(init=False, factory=WeakSet)
    _thread: Thread = field(
        init=False,
        default=Factory(lambda self: Thread(target=self._main, name=self.logger.name, daemon=True), takes_self=True),
    )

    Running: ClassVar[State] = State(initial=True)
    """
    Observing executor state:
        An EventLoopExecutor is a WaitableStateTracker whose state moves linearly, thus:
        ``EventLoopExecutor.Running`` -> ``EventLoopExecutor.Stopping`` -> ``EventLoopExecutor.Stopped``

        After construction, executors will be in the Running state until something causes them to stop (see "Stopping"(TODO)).

        Users can observe and wait for state changes by using WaitableStateTracker's `wait_for_state` method, e.g.:

        >>> executor = EventLoopExecutor()
        >>> await executor.wait_for_state(executor.Stopping)

        If users want to externally react to worker state changes, the AwaitableFutures returned from wait_for_state can
        be used directly, or subscribed to via AwaitableFuture.add_done_callback.
    """
    Stopping: ClassVar[State] = State(sources=Running)
    """This state is entered once, when :py:func:`shutdown()` is called or the worker stops for any other reason."""
    Stopped: ClassVar[State] = State(sources=Stopping)
    """This state is entered once, after the worker has shut down its event loop and has joined its captive thread."""


    def __attrs_post_init__(self):
        WaitableStateTracker.__init__(self)
        _fork_safety.workers.ensure_registered(self)  # Before the thread starts, so the fork handler knows of every worker running
        self._thread.start()
        self._runner.result()  # So that, once constructed, the worker is running

    @cached_property
    def _loop(self) -> asyncio.AbstractEventLoop:
        # Made by the fields that need it, as the worker's constructed, so on one thread. Run by _main() on the worker's
        # thread: until then, work is just queued on it.
        loop = self._loop_factory()
        if not isinstance(loop, asyncio.AbstractEventLoop):
            raise TypeError(f"loop_factory must make a new event loop, not {loop!r}")
        if loop.is_running() or loop.is_closed():
            raise TypeError(f"loop_factory made a running or closed event loop: {loop!r}")
        return loop

    @cached_property
    def _runtime_checker(self) -> Callable[[Any], None]:
        # Refuses blocking waits on the worker's loop, for its calls' and state waits' futures: made once, for all
        return blocking_wait_checker((self._loop,))

    def submit(
        self, func: Callable[_P, Coroutine[Any, Any, _T]], /, *args: _P.args, **kwargs: _P.kwargs
    ) -> AwaitableFuture[_T]:
        """
        Submits a function to run on the executor's event loop and thread.

        The function's returned coroutine will be scheduled concurrently with any other jobs submitted to the executor,
        as well as with the "main" argument provided to the executor's constructor, if any.

        The function will execute in a ``contextvars.Context`` copied from the caller, regardless of which thread it
        executes on, and regardless of the value of ``thread_inherit_context``.

        Args:
            func: The function to be called. It should return a coroutine, and should not unduly block (if it does, the
                worker's loop will hang just like any other event loop). Regardless of where ``submit()`` is called from,
                the returned coroutine will `always` be run in its own ``asyncio.Task``.
            *args: Arguments to be passed to "func".
            **kwargs: Keyword arguments to be passed to "func".

        Returns: an AwaitableFuture containing the result of executing "func" on the worker's captive event loop.
            Attempting to block on the returned future via AwaitableFuture.result() from inside the executor will raise
            an error if the future is not yet completed, to avoid accidental deadlock.

            Calling ``fork(2)`` will, in the child process, cancel any not-yet-completed futures returned by this method.

        Note:
            Cancelling the returned AwaitableFuture will cancel the job running on the worker's event loop. If this is
            undesirable, use asyncio.shield or similar. As with all async tasks in Python, the running job may
            choose to ignore or refuse cancellation.

        Note:
            The asyncio.task running "func" may be interrupted (cancelled) in response to executor failures or user calls
            to EventLoopExecutor.shutdown(cancel_futures=True).

        """
        # A call's future, with its job queued for _start(). Refused (raising why it stopped) once the worker has stopped:
        # _job() would refuse it too, but without waiting for the loop is better. And so _finalize(), failing what's
        # pending once the worker has stopped, after taking _condition, can't miss anything.
        in_loop = self.in_worker()
        loop = self._loop
        future = AwaitableFuture(prevent_blocking_wait_in_loops=self._runtime_checker)
        # The jobs' name, which their coroutines' and tasks' reprs show (tracebacks show the code's own, _job). Not
        # repr(fn), per call: for a partial, that's the repr of all its arguments.
        name = f"{self._job.__qualname__}[{getattr(func, '__qualname__', None) or type(func).__qualname__}]"
        context = copy_context()
        if context.get(sniffio.current_async_library_cvar):
            # Which anyio.run() sets to its backend: left as it is, a caller's on trio would have anyio, in the job,
            # take it to be on trio too. As trio's threads do, cleared, so that sniffio looks for the job's loop.
            context.run(sniffio.current_async_library_cvar.set, None)
        with self._condition:
            _, state, why = self._last_transition
            if state is not self.Running:
                raise copy_exception(why)  # A copy, as for _fail()
            self._submitted.add(future)
            # Only queues a callback, so never waits for the worker's loop. It runs in a copy of the caller's context.
            if in_loop:
                loop.call_soon(self._start, future, func, args, kwargs, name, context=context)
            else:
                loop.call_soon_threadsafe(self._start, future, func, args, kwargs, name, context=context)
        return future

    def bridge(self, func: Callable[_P, Coroutine[Any, Any, _T]]) -> Callable[_P, AwaitableFuture[_T]]:
        """
        Decorator version of submit(), for convenience.

        The decorated function will, when called, behave as though it were called via EventLoopExecutor.submit().

        Args:
            func: A function to be wrapped. Should return a coroutine, and is subject to the same behaviors and constraints
                as the parameter of the same name to EventLoopExecutor.submit().

        Returns:
            A wrapped version of "func" that takes the same arguments, but which returns an AwaitableFuture that will be
            resolved with the result of running "func" on the executor's event loop and thread.

        """
        # A partial rather than a closure, sparing each call a Python frame. Like a function, it binds as a method. No
        # keywords bound: they'd cost it its fast path, more than naming the job once, here, would save.
        return update_wrapper(partial(self.submit, func), func)

    def in_worker(self) -> bool:
        """
        Returns True if called from the worker's captive event loop thread. Returns False otherwise. Also returns False
        if the worker's thread has ``fork(2)``ed and this method is called from a child process.
        """
        # The worker's thread becomes the main thread if a fork() has occurred. Since the worker's thread is a daemon,
        # it'll never become the main thread in the parent, even if the original main thread exits.
        return current_thread() is self._thread is not main_thread()

    def wait_for_state(
        self, state: State, *states: State, prevent_blocking_wait_in_loops: LoopSelector = None, **kwargs
    ) -> AwaitableFuture[WaitableStateTracker._FutureType]:
        """Behaves identically to WaitableStateTracker.wait_for_state(), with the added invariant attempting to block on
        the result of the returned AwaitableFuture from within the executor's own event loop/thread will always raise an
        error if the future has not yet resolved."""
        # Refusing blocking waits on the worker's loop too, whatever's given: only its thread changes its state
        if prevent_blocking_wait_in_loops is None:
            prevent_blocking_wait_in_loops = self._runtime_checker  # Its one checker, rather than a new one per future
        elif prevent_blocking_wait_in_loops != "all":  # Which covers the worker's loop already
            prevent_blocking_wait_in_loops = (*prevent_blocking_wait_in_loops, self._loop)
        return super().wait_for_state(state, *states, prevent_blocking_wait_in_loops=prevent_blocking_wait_in_loops, **kwargs)

    def shutdown(self, wait=True, *, cancel_futures=False) -> AwaitableFuture[WaitableStateTracker._FutureType]:
        # TODO do better WRT return type; cache a future that's just the exception.
        r"""
        Immediately signal the executor to stop accepting jobs, and begin the process of shutting down its captive event
        loop and thread.

        Args:
            wait (bool): If True, this method will `synchronously` block until the worker has fully stopped its thread and
                event loop. If True and ``shutdown()`` is called inside the executor, an error will be raised if this
                method would block.

                Warning:
                    The default of ``wait=True`` is somewhat dangerous, as it can block the event loop of callers. That
                    default is necessary for compatibility with stdlib functions that interact with
                    :py:class:`concurrent.futures.Executor`, so it's important to be mindful.
            cancel_futures (bool): If True, jobs running on the worker (including any job started via the ``main=`` constructor
                argument) will be interrupted by cancelling the :py:class:`asyncio.Task`\ s that contain each job. If
                False, submitted jobs will not be cancelled, but the job started via the ``main=`` constructor argument
                `will` be cancelled if it has not already been cancelled for any reason.

                Note:
                    Jobs will be cancelled `each time` ``shutdown(cancel_futures=True)`` is called. Async code can be
                    cancelled multiple times in Python. This can result in multiple :py:class:`asyncio.CancelledError`\ s
                    being raised from the same job if users call ``shutdown(cancel_futures=True)`` repeatedly.

        Returns:
            An :py:class:`AwaitableFuture` produced from ``EventLoopExecutor.wait_for_state(EventLoopExecutor.Stopped)``.
            In other words, a future that resolves only when the worker has shut down its thread and event loop. The
            future will resolve to the triple ``(EventLoopExecutor.Stopped, EventLoopExecutor.Stopped, reason)``, where
            ``reason`` is an instance of :py:class:`WorkerStopped` describing the reason for the stop.

            If ``wait=True``, the returned future will be done.

            Cancelling the returned AwaitableFuture does nothing. ``shutdown()`` cannot be aborted or reversed.

            Note:
                The returned AwaitableFuture may not be unique.
        """

        if isinstance(broken := self._last_transition[2], WorkerBroken):
            raise copy_exception(broken)
        # Eagerly, before the stop is queued, so that refusing to wait stops nothing
        if wait and self.in_worker():
            raise RuntimeError("shutdown(wait=True) on the worker's own thread would wait for itself")

        def report_stop():  # On the worker's loop. Not a call, so it isn't refused once the worker has stopped: it can cut a drain short.
            if cancel_futures and len(self._calls) > 0:
                self._unclean_stop("manually stopped")
            else:
                self._clean_stop("stopped")
            if cancel_futures or len(self._calls) == 0:
                self._interrupt(cancel_futures)  # Otherwise the last call to end does: see _job()

        with suppress(RuntimeError):  # The loop has closed, so the worker has already stopped
            self._loop.call_soon_threadsafe(report_stop)  # Queued even on the worker's thread, behind calls made before it
        stopped = self.wait_for_state(self.Stopped)
        if wait:
            stopped.result()
        return stopped

    def _start(self, future: AwaitableFuture, func, args, kwargs, name: str):
        # If the job was cancelled by its caller or failed by the worker e.g. forking, do nothing.
        if future.done():
            return
        job = self._job(func, args, kwargs, future)
        job.__qualname__ = name
        # eager_start=True ensures that the job is queued to run before anything dispatched after it. Not setting this
        # leads to confusing priority semantics between jobs started inside the worker vs. outside.
        task = self._loop.create_task(job, eager_start=True)

        def forward(fut):
            if fut.cancelled():

                def cancel():
                    # Not once the worker has interrupted it: an interrupt also cancelling what awaits it (the parent task's
                    # TaskGroup cancelling its children, say) mustn't cancel it twice.
                    if not (self._interrupted and task.cancelling()):
                        task.cancel()

                try:
                    self._loop.call_soon_threadsafe(cancel)
                except RuntimeError:  # The loop has closed
                    pass

        future.add_done_callback(forward)

    def _fail(self, future: AwaitableFuture, error: BaseException | None = None) -> bool:
        # Else a copy of why the worker stopped, per future: each raise adds its frames to the exception's traceback
        return future.set_exception_if_not_done(error or copy_exception(self._last_transition[2]))

    def _log(self, level: int, msg: str, *args, **kwargs):
        # Only on the worker's thread: in a forked process, a handler's stream may be locked forever
        if self.in_worker():
            # Naming what recorded the stop: past this, _after_state_change(), the two transition methods, and
            # _clean_stop() or _unclean_stop()
            self.logger.log(level, msg, *args, stacklevel=6, **kwargs)

    def _after_state_change(self, old_state: State, new_state: State, data: Any):
        # Under _condition, so only the first stop (the one that enters Stopping) logs
        if new_state is self.Stopping:
            if isinstance(data, CleanStop):
                self._log(logging.INFO, data.args[0])
            else:
                self._log(logging.ERROR, "Stopping due to error:", exc_info=data)

    # Both return whether this was the first stop, so recorded: only ever on the worker's own thread
    def _clean_stop(self, why: Literal["expired", "stopped", "parent task returned"]) -> bool:
        match why:
            case "expired":
                assert len(self._calls) == 0, "Invariant violation: expiry with calls in flight"
                reason = "(idle) expired"
            case "stopped":
                reason = "(waiting for running jobs) manually stopped" if len(self._calls) > 0 else "(idle) manually stopped"
            case "parent task returned":
                reason = f"({'interrupted' if len(self._calls) > 0 else 'idle'}) parent task returned"
        return self._try_transition(self.Stopping, CleanStop(reason))

    def _unclean_stop(self, reason: str, cause: BaseException | None = None) -> bool:
        why = WorkerStopped(f"({'interrupted' if len(self._calls) > 0 else 'idle'}) {reason}", cause)
        return self._try_transition(self.Stopping, why)

    def _after_fork_in_child(self):
        """
        In the child process of a fork, EventLoopExecutor instances are invalidated. This is true even if code running
        on an EventLoopExecutor's thread did the fork; asyncio's at-fork semantics are cloudy enough that it's not worth
        trying to preserve running workers' states after fork--and that's without getting into what happens when the
        parent and child both try to interact with a worker!

        When fork occurs, this method ensures the following is true in the child process for every EventLoopExecutor
        that existed when fork(2) occurred:
        - The worker gets a fresh lock (in case another thread was using it: dispatching a @bridge call, say, at the
          moment of fork(2)), as its pending AwaitableFutures have fresh conditions already, from their own fork
          handler, which runs first (see _fork_safety).
        - The worker is Stopped, going through Stopping if it hadn't stopped, with a WorkerForked as why, unless it had
          shut down already. Its calls' futures fail with it, and its state futures resolve: neither are cancelled by the
          fork, but their callbacks don't run here, unless added with run_in_forked_child=True.
        """
        self._condition = RLock()
        forked = WorkerForked()
        self._try_transition(self.Stopping, forked)  # Unless it had stopped
        if self._try_transition(self.Stopped, forked):  # Unless it had shut down
            self._finalize()

    def _finalize(self):
        # Wait for any jobs that were being submitted at the moment of shutdown to finish being submitted. Because
        # the worker has stopped already everywhere this method is called, serializing here ensures that any submits
        # after this point in time will observe that and fail.
        with self._condition:
            to_abort = list(self._submitted)
            self._submitted.clear()
        while len(to_abort) > 0:
            self._fail(to_abort.pop())  # Never run, or abandoned (or here, in a forked process)

    def _main(self):
        async def run():
            self._expiry.reset(self._idle_timeout)  # "Reset that clock!" - Idris Elba
            self._runner.set_result(asyncio.current_task())
            await self._parent_task(self)

        loop = self._loop
        try:
            try:
                # Explicitly create a task for run(), so we can manually set the context (otherwise it's dependent on
                # the widely-considered-surprising sys.thread_inherit_context flag).
                loop.run_until_complete(loop.create_task(run(), context=Context()))
            except (Exception, asyncio.CancelledError) as ex:
                # If a stop came first, it has recorded why already, so this does nothing. Otherwise, the parent task
                # raised (CancelledError included) of its own accord.
                stopping = self._unclean_stop(f"parent task raised {ex!r}", ex)
            else:
                stopping = self._clean_stop("parent task returned")
            if stopping:
                # The parent task ending was the stop, so it interrupts calls, as shutdown(cancel_futures=True) would
                self._interrupt(True)
            # Let calls still in flight finish (or, if they were interrupted, clean up) before re-cancelling them and
            # aborting them.
            if len(self._calls) > 0:
                loop.run_until_complete(asyncio.wait(self._calls.copy()))
        except BaseException as exc:
            # Should never happen, except for a KeyboardInterrupt or SystemExit from the parent task or a call: asyncio
            # lets those escape the loop from any task. Record the crash (logging it, if it's the first stop), so that
            # calls refused from now on say why.
            self._unclean_stop("the worker's event loop crashed", exc)
            raise
        finally:
            # If run() couldn't launch at all for whatever reason (e.g. a user-supplied event loop that could not be
            # run), fail the runner future. The constructor waits on that future as a signal that the worker's ready;
            # failures here will crash the constructor with an informative error.
            self._fail(self._runner)
            # Not in a process forked from a call: its copy of the loop would run cleanup that talks over the parent's
            # sockets, and closing it may unregister fds from the epoll it shares with the parent
            in_worker = self.in_worker()
            try:
                if in_worker:
                    # TODO log the non-call/non-run tasks this cancels
                    tasks = asyncio.all_tasks(loop)
                    for task in tasks:
                        if task.cancel():
                            # Calls among them (after a crash) tell their waiters why: see _job()
                            self._interrupted = True
                    # TODO: log the threads this abandons, per worker: asyncio only warns, with a RuntimeWarning shown
                    # once.
                    # Created before the wait below starts its clock, so its own timeout fires (and warns) first.
                    # TODO some event loops may not accept timeout for executor shutdown, deal with that
                    tasks.add(loop.create_task(loop.shutdown_default_executor(timeout=0.05)))
                    tasks.add(loop.create_task(loop.shutdown_asyncgens()))
                    loop.run_until_complete(asyncio.wait(tasks, timeout=0.05))
            finally:
                self._finalize()
                self._calls.clear()
                if in_worker:  # Else a process forked from a call, whose fork handler has finished it
                    self._loop.close()  # Shuts the executor down without waiting, if it hasn't been already
                    # The very last action: shutdown()'s waiters wait for it
                    self._transition(self.Stopped, self._last_transition[2])

    def _interrupt(self, again: bool):
        # Once the worker has stopped: cancels the parent task and every call in flight. Each time with `again` (for
        # shutdown(cancel_futures=True), expiry, and the parent task ending of its own accord); otherwise (for a stop that
        # doesn't interrupt, once no calls are left in flight) only if the worker hasn't interrupted anything yet.
        if again or not self._interrupted:
            for task in {self._runner.result(timeout=0), *self._calls}:
                if task.cancel():
                    self._interrupted = True

    async def _job(self, func, args, kwargs, future: AwaitableFuture):
        """
        Runs a call on the worker's event loop. All calls are submitted by _start().
        Only raises in the thread on a crash (e.g. a cancel race or a broken event loop); all errors from user code are
        forwarded back to the user if the user is still accepting responses (that is, the waiting future hasn't been
        cancelled or aborted with an interrupt error).
        """

        # We're a little precious about performance in this method since it's on the hot path:
        task, calls, expiry = asyncio.current_task(), self._calls, self._expiry
        if self.state is not self.Running:
            self._fail(future)  # Refused, as the worker stopped after the call was made, but before its job started
            return
        calls.add(task)
        expiry.reset(None) # Suspend the idle timeout
        try:
            result = await func(*args, **kwargs)
        except asyncio.CancelledError as ex:
            # Check if we got a CancelledError because the worker was interrupted with shutdown(cancel_futures=True). If
            # so, this job's waiters get an error indicating where the job was cancelled, with exception-chain-context
            # about why the worker stopped.
            if self._interrupted and task.cancelling():
                error = WorkerInterrupted.from_cancellation(ex, copy_exception(self._last_transition[2]))
            else:
                error = ex
            # If we can't report it to the user, crash the task so Python's logger reports the failure
            if not self._fail(future, error):
                raise
        except (KeyboardInterrupt, SystemExit) as ex:
            # These two exception types are specially handled by CPython:
            # 1. They're handled in the interpreter in a racy way, so copy them.
            # 2. They always signal thread/loop-wide crash to the runtime, so they're always reraised.
            self._fail(future, copy_exception(ex))
            raise
        except BaseException as ex:
            # If we can't report the error to the user, or if it's a non-CancelledError BaseException, reraise:
            if not self._fail(future, ex) or not isinstance(ex, Exception):
                raise
        else:
            # Transmit a successful result to the user.
            # NB: if the caller cancelled it, this raises concurrent.futures.InvalidStateError, which asyncio logs as
            # never retrieved only if the job swallowed the cancellation: if the result won a race with it, the cancel()
            # that forward() queues clears the finished task's _log_traceback.
            # TODO think more about this: whether, and when, users should see that error
            future.set_result(result)
        finally:  # Once the future is resolved: a drain's interrupt below may cancel the parent task awaiting this call
            calls.remove(task)
            # A drain (a stop that leaves calls in flight to finish) cancels the parent task only once the last has
            # ended: here. Nothing else would, unless the parent task ended of its own accord, or a later shutdown()
            # interrupted.
            if len(calls) == 0:
                # Not a callback on wait_for_state(Stopping): that fires once, as the stop is recorded (begin() covers
                # there being no calls then), on whichever thread records it, rather than as the last call ends.
                if self.state is not self.Running:
                    self._interrupt(False)
                expiry.reset(self._idle_timeout)