import asyncio
import concurrent.futures
import logging
from contextlib import suppress
from contextvars import Context, ContextVar, copy_context
from datetime import timedelta
from functools import cached_property, partial, update_wrapper
from inspect import isawaitable
from threading import Thread, current_thread, main_thread
from typing import Any, Callable, ClassVar, Coroutine, Final, Literal, ParamSpec, TypeVar, final, Collection

from attrs import Factory, define, field, setters, validators
from sniffio import current_async_library_cvar

from async_anywhere._util import BlockingChecker, ConditionProtected, validate_coroutine_function, Toggleable, \
    wait_forever
from async_anywhere.exceptions import CleanStop, EventLoopExecutorBroken, JobCancelledByExecutorShutdown, EventLoopExecutorStopped, EventLoopExecutorForked
from async_anywhere.executors import _fork_safety
from async_anywhere.executors._job import _ExecutorJob
from async_anywhere.executors._timer import Timer
from async_anywhere.futures import AwaitableFuture
from async_anywhere.state_tracker import State
from async_anywhere.state_tracker.waitable import WaitableStateTracker

# Not PEP 695 type params, which Sphinx mistakes for typing's own TypeVars and drops from docs. TODO: file Sphinx bug
P = ParamSpec("P")
T = TypeVar("T")

def _seconds(timeout: float | timedelta) -> float:
    return timeout.total_seconds() if isinstance(timeout, timedelta) else timeout

def _timer_factory(executor: EventLoopExecutor):
    return Timer(
        loop=executor._loop,
        callback=lambda: executor._clean_stop("expired") and executor._interrupt(True),
    )


# Set by each executor in every context it runs code in, on its own thread: see EventLoopExecutor.current()
_current_executor: ContextVar[EventLoopExecutor | None] = ContextVar("current_executor", default=None)


@final
@define(
    kw_only=True,
    eq=False, # Make equality and hashing use object identity alone.
    on_setattr=setters.frozen, # Immutable fields. Not frozen=True, which would freeze the StateTracker's state too.
    repr=False, # Repr gets extremely verbose and not useful.
    auto_attribs=False,
)
class EventLoopExecutor(concurrent.futures.Executor, WaitableStateTracker):
    r"""An executor which contains a single thread running an event loop, and can run asynchronous code on that loop.

    The captive thread is always a daemon thread, is started when an EventLoopExecutor is constructed, and is never
    replaced.

    Running :term:`job`\ s on an executor produces :py:class:`.AwaitableFuture`\ s which can be waited or ``await``\ ed on from any
    thread or event loop. The caller and the executor itself may
    :ref:`cancel <cancellation>` those jobs.

    An executor runs until :ref:`stopped <stopping>`, after which it will not accept new
    jobs. In-progress jobs and the :term:`main task` may be cancelled, or may continue running, depending on how a stop was triggered.

    Note:
        Every method in an Executor's public API can be called (and its result waited for, if it returns a future) from
        `any` thread, process, or event loop context.
        This includes things like calling ``.submit()`` from the executor's own loop and thread.

    Example:
        Jobs can be submitted to an executor via ``.submit()`` or via the ``@bridge`` decorator:

        >>> executor = EventLoopExecutor()
        >>>
        >>> async def foo(arg, kwarg=None):
        >>>     return 6
        >>>
        >>> @executor.bridge
        >>> async def bar(arg, kwarg=None):
        >>>     return 7
        >>>
        >>> # Runs foo() on the captive loop and returns an AwaitableFuture:
        >>> a = executor.submit(foo, "arg", kwarg=True)
        >>> # Runs bar() on the captive loop and returns an AwaitableFuture
        >>> b = bar("arg", kwarg=True)
        >>> # AwaitableFutures can be waited on synchronously:
        >>> assert (6, 7) == (a.result(timeout=1), b.result(timeout=float('inf')))
        >>> # ...or asynchronously:
        >>> assert 6 == await a
        >>> # Also works as a concurrent.futures.Executor:
        >>> executor.map(foo, [[1], [2]], timeout=1)

        Jobs will be cancelled inside the executor when the future used to wait on them is cancelled.

    Advanced usage:
        ``EventLoopExecutor``\ s have several optional features for advanced use:

        - Executors can be configured to time out and shut themselves down after some amount of idle time.
        - Users can supply a custom "main" awaitable function which will always be running as an asyncio.Task inside the
          EventLoopExecutor. If that awaitable ever completes, the executor will shut itself down.
        - Users can supply custom event loops to be used inside of the executor. This facilitates e.g. combining
          :doc:`uvloop <uvloop:index>` and other event loops within the same program.
        - Users can supply a custom logger that the ``EventLoopExecutor`` will use to emit lifecycle events.

    Example:

        >>> async def connect_and_run_forever(executor): ...
        >>>
        >>> executor = EventLoopExecutor(
        >>>    main=connect_and_run_forever,
        >>>    idle_timeout=timedelta(minutes=5),
        >>>    loop_factory=uvloop.new_event_loop,
        >>>    logger=logging.getLogger("mylogger"),
        >>> )

    Warning:
        Done callbacks added (via ``add_done_callback``) to the futures that a :py:class:`EventLoopExecutor` returns
        may run on any thread, including the executor's own. User code should not make any assumptions about what
        thread, event loop, or :py:obj:`contextvars.Context` is active when a callback runs.

        As with :py:obj:`concurrent.futures.Future` and :py:obj:`~concurrent.futures.ThreadPoolExecutor` etc., user-attached callbacks
        to the executor's futures must not block, and must not raise :py:obj:`BaseException`\ s other than
        :py:obj:`KeyboardInterrupt` or :py:obj:`SystemExit`. If they do, they risk breaking the executor's
        invariants and corrupting or deadlocking it.

    .. seealso::

        -  If you need an executor which starts lazily and/or restarts its thread after stop, see :py:class:`.SingletonEventLoopExecutor`.
        - For details about how and when an executor is stopped, see :ref:`stopping`
        - For details about how and when :term:`code in an executor` is cancelled, see
          :ref:`cancellation`
        - Executors can be used in code that calls :py:func:`os.fork`. Attempts to use an executor or interact with any
          jobs it is running or futures it has produced in a process other than the one that created it will fail immediately
          with an error. Additional care is needed when using executors with forking code, see
          :ref:`fork_safety`.
        - ``await``\ ing the result of a :term:`job` from :doc:`Trio <trio:index>` works automatically. Running :term:`code in an executor`
          that uses Trio requires some setup, since the internal machinery of EventLoopExecutor is asyncio-only
          at this time. To run Trio code in an executor, use Trio's facilities for :ref:`acting as a "guest" of another
          event loop <trio:guest-mode>`, e.g. :py:func:`trio.lowlevel.start_guest_run`.
    """
    # TODO move trio docs into their own submod

    # TODO: its validator's errors name _parent_task, not its "main" alias
    _parent_task: Callable[[EventLoopExecutor], Coroutine[Any, Any, Any]] = field(
        alias="main",
        default=wait_forever,
        validator=validate_coroutine_function
    )
    """The executor's :term:`main task`. A coroutine function (``async def``) that will be wrapped in an 
    :py:obj:`asyncio.Task` and started inside the executor during construction. When this function completes, the 
    executor will stop itself via ``shutdown(wait=False)``; "main" otherwise is treated like a long-running :term:`job`
    submitted to the executor. Jobs run on the executor will run concurrently with this function.
    
    The supplied callable will be invoked only from within a running event loop, and must take a single parameter: the
    executor itself.
    
    If no value is supplied, a coroutine that waits for ever is used by default. The default is sufficient for most 
    users: unless you need to run code in the executor that has its own stateful, asynchronous setup and main loop, you
    probably do not need to set this field. 
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
    # Makes the executor's loop, once, as it's constructed: see _loop
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
    # Where waiting for its futures would deadlock: its own thread, which it adds, and wherever else it's given (the
    # threads of a SingletonEventLoopExecutor's other executors, say, which share one)
    _blocking_checker: BlockingChecker = field(alias="_blocking_checker", factory=BlockingChecker)

    # The lock transitions take, around the calls accepted, started or not, until their jobs end: what a stop drains or
    # interrupts, and what holds off expiry. So none is added once the executor has stopped. _finalize() fails what's
    # left (if the loop drops calls unrun as it closes, say). Recursive, as transitions take it again, and callbacks may.
    _condition: ConditionProtected[set[_ExecutorJob]]
    # Whether the executor has cancelled (or, with shutdown(cancel_futures=True), is about to cancel) its main task, calls,
    # or what they left running
    _interrupted: Toggleable = field(init=False, factory=Toggleable)
    # The main task's task, once it has started, which the constructor waits for: see _main()
    _runner: AwaitableFuture[asyncio.Task] = field(init=False, factory=AwaitableFuture)
    _expiry: Timer = field(init=False, default=Factory(_timer_factory, takes_self=True))

    Running: ClassVar[Final[State]] = State(initial=True)
    """
    Observing executor state:
        An EventLoopExecutor is a WaitableStateTracker whose state moves linearly, thus:
        ``EventLoopExecutor.Running`` -> ``EventLoopExecutor.Stopping`` -> ``EventLoopExecutor.Shutdown``

        After construction, executors will be in the Running state until something causes them to stop (see "Stopping"(TODO)).

        Users can observe and wait for state changes by using WaitableStateTracker's `wait_for_state` method, e.g.:

        >>> executor = EventLoopExecutor()
        >>> await executor.wait_for_state(executor.Stopping)

        If users want to externally react to executor state changes, the AwaitableFutures returned from wait_for_state can
        be used directly, or subscribed to via AwaitableFuture.add_done_callback.
    """
    Stopping: ClassVar[Final[State]] = State(sources=Running)
    """This state is entered once, when :py:meth:`shutdown()` is called or the executor stops for any other reason."""
    Shutdown: ClassVar[Final[State]] = State(sources=Stopping)
    """This state is entered once, after the executor has shut down its event loop and has joined its captive thread."""


    def __attrs_post_init__(self):
        super().__init__()
        self._condition.replace_value(set())  # Its jobs: see _condition
        _fork_safety.executors.ensure_registered(self)
        # Captured outside the try/except in case the code to prime these fields has errors
        thread, runner = self._thread, self._runner
        try:
            thread.start()
            runner.result() # Wait for the captive thread to signal that it's ready and running the event loop
        except BaseException:
            # Any error during construction (e.g. KeyboardInterrupt) risks leaving a background thread runnong on the
            # system, expecting work. While this class can't be made interrupt-safe generally, it is easy to signal for
            # thread shutdown when loop-existential errors occur early in the executor's lifecycle, so we do that here.
            self.shutdown(wait=False)
            raise

    @cached_property
    def _loop(self) -> asyncio.AbstractEventLoop:
        # Made by the fields that need it, as the executor's constructed, so on one thread. Run by _main() on the executor's
        # thread: until then, work is just queued on it.
        loop = self._loop_factory()
        if not isinstance(loop, asyncio.AbstractEventLoop):
            raise TypeError(f"loop_factory must make a new event loop, not {loop!r}")
        if loop.is_running() or loop.is_closed():
            raise TypeError(f"loop_factory made a running or closed event loop: {loop!r}")
        return loop

    @cached_property
    def _thread(self) -> Thread:
        # Explicitly given a fresh context, holding just the executor, which run()'s task copies (otherwise it's dependent
        # on the widely-considered-surprising sys.thread_inherit_context flag). Made as __attrs_post_init__() starts it.
        context = Context()
        context.run(_current_executor.set, self)
        # TODO: setting the executor in the context early/eagerly via the context= argument to Thread causes reference
        # loops, GC load, and potentially-immortal stopped executor instances, since thread context objects are not
        # garbage collected when we need them to be.
        rv = Thread(target=self._main, name=self.logger.name, daemon=True, context=context)
        self._blocking_checker.add(rv)  # Prevent blocking waits on the captive thread
        return rv

    def submit(
        self, func: Callable[P, Coroutine[Any, Any, T]], /, *args: P.args, **kwargs: P.kwargs
    ) -> AwaitableFuture[T]:
        r"""
        Submits a function to run on the executor's thread.

        The function's returned coroutine will be scheduled concurrently with any other jobs submitted to the executor,
        as well as with the "main" argument provided to the executor's constructor, if any.

        The function will execute in a :py:obj:`contextvars.Context` copied from the caller, regardless of which
        thread it executes on, and regardless of the value of :py:obj:`sys.flags.thread_inherit_context`.

        Args:
            func: The function to be called. It should return a coroutine, and should not unduly block (if it does, the
                executor's loop will hang just like any other event loop). Regardless of where ``submit()`` is called from,
                the returned coroutine will `always` be run in its own ``asyncio.Task``.
            *args: Arguments to be passed to ``func``.
            **kwargs: Keyword arguments to be passed to ``func``.

                Note:
                    Passing any keyword arguments whose names start with an underscore (e.g. ``.submit(_foo=1)``) is
                    unsupported. Underscore-prefixed kwargs are reserved for future use by this module and may or may
                    not be passed to "func".

        Returns:
            An :py:class:`.AwaitableFuture` containing the result of executing "func" on the executor's event loop.
            Attempting to block on the returned future via :py:meth:`.AwaitableFuture.result` from inside the executor
            will raise :py:class:`.WouldBlock` if the future is not yet completed, to avoid accidental deadlock.

            Calling ``fork(2)`` will, in the child process, fail (complete with a EventLoopExecutorForked exception) any
            not-yet-completed futures that were returned by this method.

        Note:
            Cancelling the returned AwaitableFuture will cancel the job running on the executor's event loop. If this is
            undesirable, use asyncio.shield or similar. As with all async tasks in Python, the running job may
            choose to ignore or refuse cancellation.

        Note:
            The asyncio.task running "func" may be cancelled in response to executor failures
            or user calls to :py:meth:`EventLoopExecutor.shutdown(cancel_futures=True) <shutdown>`. In those cases,
            "func" may never be called at all. For more details, see :ref:`cancellation`.

        Warning:
            Avoid blocking the executor's event loop: this is "supported" insofar as its semantics are documented and
            it won't automatically cause errors or undefined behavior, but it is a bad idea. As with any
            :py:obj:`threading.Thread` that is blocked, the executor's captive thread cannot run, jobs and the main task cannot make
            progress, nor can the executor be fully shut down, while code blocks its thread.

            While :term:`code in an executor` blocks the executor's thread and event loop, users can still call
            ``.submit()``, but :term:`job`\ s will not be started until the executor is unblocked. Similarly, users can
            still signal the executor to stop (via ``.shutdown()``) while it is blocked.

        """
        # TODO fix doc render indent of kwargs parameter after https://github.com/sphinx-doc/sphinx/issues/9141 is fixed


        # TODO assess storing a partial FutureFactory of some kind
        # TODO assess storing a precreated future e.g. with the done callback _internally set.
        # TODO assess copying template futures.
        # TODO assess pre-caching some utility state e.g. on the executor thread while idle.

        (
            future,
            context,
            name,
        ) = (
            # Performance: re-use the blocking checker rather than passing in this executor's loop/thread. This path is
            # hot, so saving here has an impact.
            _ExecutorJob(
                prevent_blocking_wait_when=self._blocking_checker,
                done_callbacks=(self._cancellation_forwarder,),  # Cheaper than add_done_callback(), which locks
            ),
            copy_context(),
            # Compute job names for use in tracebacks, so users see exceptions as arising from
            # "EventLoopExecutor._job[user.function.name]". This helps distinguish failures when multiple jobs are
            # running on the executor.
            f"{self._job.__qualname__}[{getattr(func, '__qualname__', None) or type(func).__qualname__}]"
        )
        job = self._job(func, args, kwargs, future)  # Nothing in it runs until future.start()
        job.__qualname__, future._target = name, job

        context.run(_current_executor.set, self)  # In the call's context, which its job's task copies

        # If a caller is using a non-asyncio/stdlib runtime (e.g. anyio, trio), remove that preference from the context.
        # Code inside the executor assumes asyncio/stdlib (and can host other runtimes via runtime-specific tools like
        # e.g. Trio guest loops), so running jobs coded to non-asyncio/stdlib async APIs will fail. Clearing this out
        # hints to backend-agnostic async systems that asyncio/stdlib should be used, and (hopefully) makes
        # "not running in the right non-stdlib async system" errors raised by backend-specific systems clearer:
        if context.get(current_async_library_cvar):
            context.run(current_async_library_cvar.set, None)

        try:
            with self._condition as jobs:
                # TODO can we easily do this opportunistically outside the lock? Is that worth it since its error is a
                #   misuse condition anyway?
                if self.state is not self.Running:
                    future.finish()  # Closing its coroutine, never to run
                    raise self.last_transition[2].copy()

                # Accepted for good: from now on the job runs, or is interrupted, but is never refused
                jobs.add(future)

            # Unlocked: asyncio's self-pipe send() drops the GIL, so holding the lock across it stalls a loop that takes it
            if self.is_current_executor():
                # TODO can we just call start directly and chain the future? Tried that before, but why was it abandoned?
                self._loop.call_soon(future.start, self._loop, context=context)
            else:
                try:
                    self._loop.call_soon_threadsafe(future.start, self._loop, context=context)
                except RuntimeError:  # The loop closed after a crash, and _finalize() has failed the job
                    pass
        except BaseException:  # A signal's KeyboardInterrupt, say: else a job accepted but never posted stalls any stop
            with self._condition as jobs:
                accepted = future in jobs
                jobs.discard(future)
            if accepted:  # Its start may be posted, so cancelled for that to skip it (or for its task, if running)
                future.cancel()
            raise
        return future

    def bridge(self, func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, AwaitableFuture[T]]:
        """
        Decorator version of :py:meth:`submit`, for convenience.

        Args:
            func: A function to be wrapped. Should return a coroutine, and is subject to the same behaviors and constraints
                as the parameter of the same name to ``EventLoopExecutor.submit()``.

        Returns:
            A wrapped version of ``func`` that takes the same arguments, but which returns an ``AwaitableFuture`` that
            will be :term:`completed` with the result of running ``func`` on the executor's event loop and thread.

        """
        # Performance: using a partial instead of a closure saves a frame and speeds up calls a bit.
        # NB: decorated things will be bound as methods (TODO: right? Or "equivalent" to methods without descriptors?)
        # NB: we're not precomputing parts of submit's code here (e.g. the name) because it'd require a kwarg which
        #     takes partial invocation off of the fast call path.
        return update_wrapper(partial(self.submit, func), func)

    @classmethod
    def current(cls) -> EventLoopExecutor | None:
        """
        Returns the executor whose captive :py:obj:`threading.Thread` this method is called from, or None if called
        from a thread that is not owned by an ``EventLoopExecutor``.

        Note:
            This method returns the executor that owns the calling thread regardless of the state of that executor.
            If the executor is stopping, has forked into a child process, is running destructor code after its
            event loop has shutdown, `et cetera`, this function will still return it.
        """
        executor = _current_executor.get()
        if executor is not None and executor._thread is current_thread():
            return executor
        return None

    def is_current_executor(self) -> bool:
        """
        Returns True if called from this executor's captive event loop thread, i.e. if :py:meth:`current` would return
        this executor. Returns False otherwise.
        """
        # Performance: inline parts of .current() since this is on the hot path of submit/job execution.
        return _current_executor.get() is self and current_thread() is self._thread

    def current_job(self) -> AwaitableFuture | None:
        """
        Returns:
            The :py:class:`.AwaitableFuture` of the invocant executor's :term:`job` that this method is called from
            (the same one :py:func:`submit` returned for it), or None if it is not called from one of the executor's
            jobs.

            Code is inside a job if it is run by a call to a function called via :py:func:`submit`. If code is
            *dispatched* from a job but is not directly *called* by it (e.g. if the job backgrounds an ``asyncio.Task``
            or runs work on a thread via ``asgiref`` or ``asyncio.to_thread``), this method returns None.

            Cancelling the returned future cancels the job, just as its caller cancelling it would. Do not call
            ``set_result()`` or ``set_exception()`` on it: the job's caller would get that result or error, and the
            job's own outcome would be logged as an error once the job ends.
        """
        loop = asyncio._get_running_loop()
        if loop is not self._loop or (task := asyncio.current_task(loop)) is None:  # Jobs are tasks on this loop
            return None
        calls, coroutine = {}, task.get_coro()
        with self._condition as jobs:  # As submit() adds jobs on other threads
            for job in jobs:
                if job._target is coroutine:  # In its first step, which runs as it starts: see _ExecutorJob.start()
                    return job
                if (started := job.task) is not None:
                    calls[started] = job
        if (future := calls.get(task)) is not None:
            return future
        # Else a job awaiting this, maybe via other tasks: asyncio records who awaits what, for capture_call_graph()
        seen, todo = {task}, [task]
        while len(todo) > 0:
            for awaiter in todo.pop()._asyncio_awaited_by or ():
                if (future := calls.get(awaiter)) is not None:
                    return future
                if awaiter not in seen:  # Tasks deadlocked awaiting each other would loop forever
                    seen.add(awaiter)
                    todo.append(awaiter)
        return None

    @property
    def _forked(self) -> bool:
        """
        Checks if the executor has been forked into another process.

        Note:
            The executor's thread becomes the main thread if a fork() has occurred. Since the executor's thread is a daemon,
            it'll never become the main thread in the parent, even if the original main thread exits.

        Returns:
            True if the executor is in a child (or grandchild) process of the process in which it was created. A True
            return value doesn't indicate anything about executor state or usability.

        """
        # WARNING: in certain cases, this is vulnerable to PID wraparound. Its current callers are not vulnerable, but
        # addition additional ones might be.
        # TODO assess PID wraparound risk.
        return self._thread is main_thread()

    # TODO doc inherit signature somehow; we don't care about the signature in this subclass override.
    def wait_for_state(
        self,
        state: State,
        *states: State,
        prevent_blocking_wait_when: Collection[asyncio.AbstractEventLoop | Thread] | None = None,
        **kwargs,
    ):
        """Behaves identically to :py:meth:`.WaitableStateTracker.wait_for_state`, with the added invariant that
        attempting to block on the returned :py:class:`.AwaitableFuture` from within the executor's own event
        loop/thread will always raise an :py:class:`.WouldBlock` (to prevent accidental deadlock) if the future is not
        yet done.
        """
        checker = self._blocking_checker
        if prevent_blocking_wait_when is not None:
            checker = checker.copy(add=prevent_blocking_wait_when)
        return super().wait_for_state(state, *states, prevent_blocking_wait_when=checker, **kwargs)

    def shutdown(self, wait=True, *, cancel_futures=False) -> AwaitableFuture[EventLoopExecutorStopped]:
        r"""
        Immediately signal the executor to stop accepting jobs, and begin the process of shutting down its captive event
        loop and thread. See :ref:`stopping` for details.

        Args:
            wait (bool): If True, this method will `synchronously` block until the executor has fully stopped its thread and
                event loop. If True and ``shutdown()`` is called inside the executor, an error will be raised if this
                method would block.

                Warning:
                    The default of ``wait=True`` is somewhat dangerous, as it can block the event loop of callers. That
                    default is necessary for compatibility with stdlib functions that interact with
                    :py:obj:`concurrent.futures.Executor`, so it's important to be mindful.
            cancel_futures (bool): If True, :term:`job`\ s running on the executor and its :term:`main task`
                will be :ref:`cancelled <cancellation>` (by calling ``.cancel()`` on the :py:obj:`asyncio.Task`\ s that contain them). If
                False, jobs will not be cancelled, but the main task
                `will` be cancelled if it has not already been cancelled for any reason.

                Note:
                    Jobs and the main task will be cancelled `each time` ``shutdown(cancel_futures=True)`` is called. Async code can be
                    cancelled multiple times in Python. This can result in multiple :py:obj:`asyncio.CancelledError`\ s
                    being raised from the same task if users call ``shutdown(cancel_futures=True)`` repeatedly.

        Returns:
            An :py:class:`.AwaitableFuture` produced from ``EventLoopExecutor.wait_for_state(EventLoopExecutor.Shutdown)``.
            In other words, a future that resolves only when the executor has shut down its thread and event loop. The
            future will resolve to an instance of :py:class:`.EventLoopExecutorStopped` describing the reason for the stop.

            If ``wait=True``, the returned future will be done.

            Cancelling the returned AwaitableFuture does nothing. ``shutdown()`` cannot be aborted or reversed, and
            takes effect immediately when this function is called, not when its future is waited on.

            Note:
                The returned AwaitableFuture may not be unique.

            Note:
                If an executor's captive thread is blocked and its event loop is not running, users can still signal the
                executor to stop via ``.shutdown()``. As soon as ``.shutdown()`` is called, the executor will refuse to
                accept any new :term:`job`\ s. When its event loop becomes unblocked, the remainder of
                :ref:`the shutdown process <stopping>` will occur.
        """

        if isinstance(broken := self.last_transition[2], EventLoopExecutorBroken):
            raise broken.copy()
        # Before requesting the shutdown, make sure we weren't asked to wait on our own thread:
        if wait and self.state is not self.Shutdown:
            self._blocking_checker.check(self.shutdown)

        # Stopped here and now, on this thread, under the lock submit() takes: so every call is either refused, or
        # accepted before the stop, to be drained or interrupted
        with self._condition as jobs:
            busy = len(jobs) > 0
            if cancel_futures and busy:
                self._unclean_stop("manually stopped")
            else:
                self._clean_stop("stopped")
            if cancel_futures:  # Once stopped, as _job() then reads why, and before the loop gets to calls not yet started
                self._interrupted.set()
        if cancel_futures or not busy:  # Otherwise the last call to end does: see _job()
            with suppress(RuntimeError):  # RuntimeError: the loop has closed, so the executor has already stopped
                self._loop.call_soon_threadsafe(self._interrupt, cancel_futures)
        stopped = self.wait_for_state(self.Shutdown)
        if wait:
            stopped.result()
        return stopped

    @property
    def _tasks(self) -> list[asyncio.Task]:
        # Started jobs' tasks
        with self._condition as jobs:  # As submit() adds jobs on other threads
            return [task for job in jobs if (task := job.task) is not None]

    @cached_property
    def _cancellation_forwarder(self) -> Callable[[_ExecutorJob], None]:
        # Each call's future's first done callback (see submit()), made once, for all: cancelling the future cancels its
        # job's task, on the loop, as its start() and _job() run there too. So no lock: a call cancelled before its job
        # starts has no task yet, and its job never runs (see _job()).

        def forward_cancellations(future: _ExecutorJob):
            if future.cancelled():

                def cancel():
                    # Not once the executor has interrupted it: an interrupt also cancelling what awaits it (the
                    # main task's TaskGroup cancelling its children, say) mustn't cancel it twice.
                    if (task := future.task) is not None and not (self._interrupted and task.cancelling()):
                        task.cancel()
                # TODO can we call it directly if on the loop?
                # TODO can we have a util method that decides call-soon/threadsafe/local-call?
                try:
                    self._loop.call_soon_threadsafe(cancel)
                except RuntimeError:  # The loop has closed
                    pass

        return forward_cancellations

    def _fail(self, future: AwaitableFuture, error: BaseException | None = None) -> bool:
        # Else a copy of why the executor stopped, per future: each raise adds its frames to the exception's traceback
        return future.set_exception_if_not_done(error or self.last_transition[2].copy())

    def _log(self, level: int, msg: str, *args, **kwargs):
        # Never in a forked process, where a handler's stream may be locked forever: its fork handler stops the executor
        if not self._forked and not isinstance(self.last_transition[2], EventLoopExecutorForked):
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

    # Both return whether this was the first stop, so recorded. Under _condition, so that no call is accepted meanwhile.
    def _clean_stop(self, why: Literal["expired", "stopped", "main task returned"]) -> bool:
        with self._condition as jobs:
            busy = len(jobs) > 0
            match why:
                case "expired":
                    if busy:  # Accepted, but not yet started, as the countdown ended: the last call to end restarts it
                        return False
                    reason = "(idle) expired"
                case "stopped":
                    reason = "(waiting for running jobs) manually stopped" if busy else "(idle) manually stopped"
                case "main task returned":
                    reason = f"({'tasks cancelled' if busy else 'idle'}) main task returned"
            return self._try_transition(self.Stopping, CleanStop(reason))

    def _unclean_stop(self, reason: str, cause: BaseException | None = None) -> bool:
        with self._condition as jobs:
            why = EventLoopExecutorStopped(f"({'tasks cancelled' if len(jobs) > 0 else 'idle'}) {reason}", cause)
            return self._try_transition(self.Stopping, why)

    def _after_fork_in_child(self):
        """
        In the child process of a fork, EventLoopExecutor instances are invalidated. This is true even if code running
        on an EventLoopExecutor's thread did the fork; asyncio's at-fork semantics are cloudy enough that it's not worth
        trying to preserve running executors' states after fork--and that's without getting into what happens when the
        parent and child both try to interact with an executor!

        When fork occurs, this method ensures the following is true in the child process for every EventLoopExecutor
        that existed when fork(2) occurred:
        - The executor's lock is reset (in case another thread was using it: dispatching a @bridge call, say, at the
          moment of fork(2)), as its pending AwaitableFutures' conditions are, by ConditionProtected's own fork handler,
          which runs first (see async_anywhere._util).
        - The executor is Shutdown, going through Stopping if it hadn't stopped, with a EventLoopExecutorForked as why, unless it had
          shut down already. Its calls' futures fail with it, and its state futures resolve: neither are cancelled by the
          fork, but their callbacks don't run here, unless added with run_in_forked_child=True.
        """
        forked = EventLoopExecutorForked()
        self._try_transition(self.Stopping, forked)  # Unless it had stopped
        if self._try_transition(self.Shutdown, forked):  # Unless it had shut down
            self._finalize()

    def _finalize(self):
        # Wait for any jobs that were being submitted at the moment of shutdown to finish being submitted. Because
        # the executor has stopped already everywhere this method is called, serializing here ensures that any submits
        # after this point in time will observe that and fail.
        with self._condition as jobs:
            to_abort = list(jobs)
            jobs.clear()
        stopped = self.last_transition[2]
        while len(to_abort) > 0:
            job = to_abort.pop()
            # Closing its coroutine, if never started: then cancelled by the stop, as _job() fails those it refuses
            if job.finish() and not isinstance(stopped, EventLoopExecutorForked):
                self._fail(job, JobCancelledByExecutorShutdown.from_cancellation(stopped))
            else:
                self._fail(job)  # Abandoned (or here, in a forked process)

    def _main(self):
        async def run():
            self._expiry.reset(self._idle_timeout)  # "Reset that clock!" - Idris Elba
            self._runner.set_result(asyncio.current_task())
            await self._parent_task(self)

        loop = self._loop
        try:
            try:
                loop.run_until_complete(loop.create_task(run()))
            except (Exception, asyncio.CancelledError) as ex:
                # If a stop came first, it has recorded why already, so this does nothing. Otherwise, the main task
                # raised (CancelledError included) of its own accord.
                stopping = self._unclean_stop(f"main task raised {ex!r}", ex)
            else:
                stopping = self._clean_stop("main task returned")
            if stopping:
                # The main task ending was the stop, so it interrupts calls, as shutdown(cancel_futures=True) would
                self._interrupt(True)
            # One turn runs the start()s posted so far, whose calls _job() fails as interrupted; _finalize() fails any never started
            loop.run_until_complete(asyncio.sleep(0))
            # Let calls still in flight finish (or, if they were interrupted, clean up) before re-cancelling them and
            # aborting them.
            if len(tasks := self._tasks) > 0:
                loop.run_until_complete(asyncio.wait(tasks))
        except BaseException as exc:
            # Should never happen, except for a KeyboardInterrupt or SystemExit from the main task or a call: asyncio
            # lets those escape the loop from any task. Record the crash (logging it, if it's the first stop), so that
            # calls refused from now on say why.
            self._unclean_stop("the executor's event loop crashed", exc)
            raise
        finally:
            # If run() couldn't launch at all for whatever reason (e.g. a user-supplied event loop that could not be
            # run), fail the runner future. The constructor waits on that future as a signal that the executor's ready;
            # failures here will crash the constructor with an informative error.
            self._fail(self._runner)
            # Not in a process forked from a call: its copy of the loop would run cleanup that talks over the parent's
            # sockets, and closing it may unregister fds from the epoll it shares with the parent
            forked = self._forked
            try:
                if not forked:
                    # TODO log the non-call/non-run tasks this cancels
                    tasks = asyncio.all_tasks(loop)
                    # Calls among them (after a crash) tell their waiters why: see _job()
                    self._interrupted.set()  # Even with none to cancel, so calls not yet started never start
                    for task in tasks:
                        task.cancel()
                    # TODO: log the threads this abandons, per executor: asyncio only warns, with a RuntimeWarning shown
                    # once.
                    # Created before the wait below starts its clock, so its own timeout fires (and warns) first.
                    # TODO some event loops may not accept timeout for executor shutdown, deal with that
                    tasks.add(loop.create_task(loop.shutdown_default_executor(timeout=0.05)))
                    tasks.add(loop.create_task(loop.shutdown_asyncgens()))
                    loop.run_until_complete(asyncio.wait(tasks, timeout=0.05))
            finally:
                self._finalize()
                if not forked:  # Else a process forked from a call, whose fork handler has finished it
                    self._loop.close()  # Shuts the executor down without waiting, if it hasn't been already
                    # The very last action: shutdown()'s waiters wait for it
                    self._transition(self.Shutdown, self.last_transition[2])
                # Last, as its callbacks may call current(): the thread keeps its context, which would keep the executor alive
                _current_executor.set(None)

    def _interrupt(self, again: bool):
        # Once the executor has stopped: cancels the main task and every call in flight. Each time with `again` (for
        # shutdown(cancel_futures=True), expiry, and the main task ending of its own accord); otherwise (for a stop that
        # doesn't interrupt, once no calls are left in flight) only if the executor hasn't interrupted anything yet.
        if again or not self._interrupted:
            self._interrupted.set()  # Even with none to cancel, so calls not yet started never start: see _job()
            for task in {self._runner.result(timeout=0), *self._tasks}:
                task.cancel()


    async def _job(self, func, args, kwargs, future: _ExecutorJob):
        """
        Runs a job on the executor's event loop. All jobs are submitted by their start().
        Only raises in the thread on a crash (e.g. a cancel race or a broken event loop); all errors from user code are
        forwarded back to the user if the user is still accepting responses (that is, the waiting future hasn't been
        cancelled or failed with a JobCancelledByExecutorShutdown).
        """

        # We're a little precious about performance in this method since it's on the hot path:
        task, expiry = asyncio.current_task(), self._expiry
        expiry.reset(None) # Suspend the idle timeout
        try:
            # If the job was cancelled by its caller or failed by the executor e.g. forking, do nothing.
            if future.done():
                return
            if self._interrupted:  # Accepted before an interrupt, which it never starts after: see shutdown()
                self._fail(future, JobCancelledByExecutorShutdown.from_cancellation(self.last_transition[2]))
                return
            result = func(*args, **kwargs)
            if isawaitable(result):
                result = await result
        except asyncio.CancelledError as ex:
            # Check if we got a CancelledError because the executor was interrupted with shutdown(cancel_futures=True). If
            # so, this job's waiters get an error indicating where the job was cancelled, with exception-chain-context
            # about why the executor stopped.
            if self._interrupted and task.cancelling():
                error = JobCancelledByExecutorShutdown.from_cancellation(ex, self.last_transition[2])
            else:  # Asked or not: asyncio ends a task cancelled when a CancelledError escapes it, so its future too
                future.cancel()
                raise
            # If we can't report it to the user, crash the task so Python's logger reports the failure
            if not self._fail(future, error):
                raise
        except (KeyboardInterrupt, SystemExit) as ex:
            # These two exception types are specially handled by CPython:
            # 1. They're handled in the interpreter in a racy way, so copy them.
            # 2. They always signal thread/loop-wide crash to the runtime, so they're always reraised.
            self._fail(future, EventLoopExecutorStopped.copy_exception(ex))
            raise
        except BaseException as ex:
            # If we can't report the error to the user, or if it's a non-CancelledError BaseException, reraise:
            if not self._fail(future, ex) or not isinstance(ex, Exception):
                raise
        else:
            # Transmit a successful result to the user.
            # NB: if the caller cancelled it, this raises concurrent.futures.InvalidStateError, which asyncio logs as
            # never retrieved only if the job swallowed the cancellation: if the result won a race with it (so the
            # cancellation never reached the task), it's dropped.
            # TODO think more about this: whether, and when, users should see that error
            if task._must_cancel:  # Cancelled unseen as it returned: asyncio ends the task cancelled, so its future too
                future.cancel()
            elif (not future.cancelled()) or (task.cancelling() > 0):
                future.set_result(result)
        finally:  # Once the future is resolved: a drain's interrupt below may cancel the main task awaiting this call
            future.finish()
            with self._condition as jobs:
                jobs.discard(future)
                idle, stopping = len(jobs) == 0, self.state is not self.Running
            # A drain (a stop that leaves calls in flight to finish) cancels the main task only once the last has
            # ended: here. Nothing else would, unless the main task ended of its own accord, or a later shutdown()
            # interrupted.
            if idle:
                # Not a callback on wait_for_state(Stopping): that fires once, as the stop is recorded (begin() covers
                # there being no calls then), on whichever thread records it, rather than as the last call ends.
                if stopping:
                    self._interrupt(False)
                expiry.reset(self._idle_timeout)