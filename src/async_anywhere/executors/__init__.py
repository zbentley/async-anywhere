import asyncio
import concurrent.futures
import logging
from contextlib import suppress
from contextvars import Context, ContextVar, copy_context
from datetime import timedelta
from functools import cached_property, partial, update_wrapper
from inspect import isawaitable
from threading import Thread, current_thread, main_thread, RLock
from typing import Any, Callable, ClassVar, Coroutine, Literal, ParamSpec, TypeVar, final, Collection
from weakref import WeakKeyDictionary

from attrs import Factory, define, field, setters, validators
from sniffio import current_async_library_cvar

from async_anywhere._util import BlockingChecker, validate_coroutine_function, Toggleable
from async_anywhere.exceptions import CleanStop, EventLoopExecutorBroken, JobCancelledByExecutorShutdown, EventLoopExecutorStopped, EventLoopExecutorForked
from async_anywhere.executors import _fork_safety
from async_anywhere.executors._timer import Timer
from async_anywhere.futures import AwaitableFuture
from async_anywhere.state_tracker import State
from async_anywhere.state_tracker.waitable import WaitableStateTracker

# Not PEP 695 type params, which Sphinx mistakes for typing's own TypeVars and drops from docs. TODO: file Sphinx bug
P = ParamSpec("P")
T = TypeVar("T")


async def _wait_forever(_):
    await asyncio.Future()

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
        >>> async def foo(arg, kwarg=None): ...
        >>>
        >>> @executor.bridge
        >>> async def bar(arg, kwarg=None): ...
        >>>
        >>> executor.submit(foo, "arg", kwarg=True)  # Runs foo() on the captive loop and returns an AwaitableFuture
        >>> bar("arg", kwarg=True) # Runs bar() on the captive loop and returns an AwaitableFuture
        >>> executor.map(foo, [[1], [2]], timeout=1)  # Works as a concurrent.futures.Executor

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
          event loop <trio:guest-mode>`, e.g. :py:func:`trio.lowlevel.start_guest_run`. See tests/test_executors/test_runtimes.py for examples.
    """
    # TODO move trio docs into their own submod

    # TODO: its validator's errors name _parent_task, not its "main" alias
    _parent_task: Callable[[EventLoopExecutor], Coroutine[Any, Any, Any]] = field(
        alias="main",
        default=_wait_forever,
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

    # Bridged calls in flight: their jobs' tasks. They hold off idle expiry, and drains wait for them; see _interrupt().
    _calls: set[asyncio.Task] = field(init=False, factory=set)
    # Whether the executor has cancelled its main task, calls, or what they left running
    _interrupted: Toggleable = field(init=False, factory=Toggleable)
    # The main task's task, once it has started, which the constructor waits for: see _main()
    _runner: AwaitableFuture[asyncio.Task] = field(init=False, factory=AwaitableFuture)
    _expiry: Timer = field(init=False, default=Factory(_timer_factory, takes_self=True))
    # Calls' futures, which _finalize() fails if the executor won't finish them (if the loop drops them unrun as it closes,
    # say), each with its job's task once _start() has made it (see _cancellation_forwarder). Weak, as a call that's
    # done, and dropped by its caller, needs nothing more. Added to, and popped from, only under _condition, which
    # transitions take too, so none is added once the executor has stopped; _start() just sets tasks, on the executor's
    # thread, which _finalize() lists them on.
    _submitted: WeakKeyDictionary[AwaitableFuture, asyncio.Task | None] = field(init=False, factory=WeakKeyDictionary)

    Running: ClassVar[State] = State(initial=True)
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
    Stopping: ClassVar[State] = State(sources=Running)
    """This state is entered once, when :py:meth:`shutdown()` is called or the executor stops for any other reason."""
    Shutdown: ClassVar[State] = State(sources=Stopping)
    """This state is entered once, after the executor has shut down its event loop and has joined its captive thread."""


    def __attrs_post_init__(self):
        super().__init__()
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
            or user calls to :py:meth:`EventLoopExecutor.shutdown(cancel_futures=True) <shutdown>`. For more details, see
            :ref:`cancellation`.

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
            AwaitableFuture(prevent_blocking_wait_when=self._blocking_checker),
            copy_context(),
            # Compute job names for use in tracebacks, so users see exceptions as arising from
            # "EventLoopExecutor._job[user.function.name]". This helps distinguish failures when multiple jobs are
            # running on the executor.
            f"{self._job.__qualname__}[{getattr(func, '__qualname__', None) or type(func).__qualname__}]"
        )

        future.add_done_callback(self._cancellation_forwarder)

        # If a caller is using a non-asyncio/stdlib runtime (e.g. anyio, trio), remove that preference from the context.
        # Code inside the executor assumes asyncio/stdlib (and can host other runtimes via runtime-specific tools like
        # e.g. Trio guest loops), so running jobs coded to non-asyncio/stdlib async APIs will fail. Clearing this out
        # hints to backend-agnostic async systems that asyncio/stdlib should be used, and (hopefully) makes
        # "not running in the right non-stdlib async system" errors raised by backend-specific systems clearer:
        if context.get(current_async_library_cvar):
            context.run(current_async_library_cvar.set, None)

        with self._condition:
            # TODO can we easily do this opportunistically outside the lock? Is that worth it since its error is a
            #   misuse condition anyway?
            if self._last_transition[1] is not self.Running:
                raise self._last_transition[2].copy()

            # _submitted maps futures to None (when they haven't been picked up by the executor yet), and then later to
            # asyncio.Tasks when they're in progress:
            self._submitted[future] = None

            if self.is_current_executor():
                # TODO can we just call _start directly and chain the future? Tried that before, but why was it abandoned?
                self._loop.call_soon(self._start, future, func, args, kwargs, name, context=context)
            else:
                self._loop.call_soon_threadsafe(self._start, future, func, args, kwargs, name, context=context)
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

        if isinstance(broken := self._last_transition[2], EventLoopExecutorBroken):
            raise broken.copy()
        # Before requesting the shutdown, make sure we weren't asked to wait on our own thread:
        if wait and self.state is not self.Shutdown:
            self._blocking_checker.check(self.shutdown)

        # TODO transition to Stopping synchronously so we won't have races.
        def report_stop():
            """
            On the executor's captive loop, perform the shutdown: record the exception and cancel jobs or the main task if
            appropriate.

            Doesn't use :py:meth:`submit` or its machinery so it can be exempt from all internal checks aimed at
            preventing code from running on a broken or stopping executor. ``shutdown()`` is the exception to those rules.
            """

            # Since we're not using .submit(), adjust the context by hand (otherwise the whole context is inherited from
            # the caller).
            _current_executor.set(self)
            if cancel_futures and len(self._calls) > 0:
                self._unclean_stop("manually stopped")
            else:
                self._clean_stop("stopped")
            if cancel_futures or len(self._calls) == 0:
                self._interrupt(cancel_futures)  # Otherwise the last call to end does: see _job()

        with suppress(RuntimeError):  # RuntimeError: the loop has closed, so the executor has already stopped
            # Queue shutdown, even if called from the executor's thread, so that it is begun after any other in-flight
            # calls are accepted by the event loop.
            # TODO can we do better than that without risking racy state corruption?
            self._loop.call_soon_threadsafe(report_stop)
        stopped = self.wait_for_state(self.Shutdown)
        if wait:
            stopped.result()
        return stopped

    def _start(self, future: AwaitableFuture, func, args, kwargs, name: str):
        # If the job was cancelled by its caller or failed by the executor e.g. forking, do nothing.
        if future.done():
            return
        _current_executor.set(self)  # In the call's context (see submit()), which its job's task copies
        job = self._job(func, args, kwargs, future)
        job.__qualname__ = name
        # eager_start=True ensures that the job is queued to run before anything dispatched after it. Not setting this
        # leads to confusing priority semantics between jobs started inside the executor vs. outside.
        self._submitted[future] = self._loop.create_task(job, eager_start=True)
        if future.cancelled():  # Since done() above, so before _cancellation_forwarder could find the task
            self._cancellation_forwarder(future)

    @cached_property
    def _cancellation_forwarder(self) -> Callable[[AwaitableFuture], None]:
        # Each call's future's first done callback (see submit()), made once, for all: cancelling the future cancels its
        # job's task, once _start() has made it. The task's popped, so that this and _start() cancel it only once
        # between them; under _condition, as _finalize() lists them under it.

        def forward_cancellations(future: AwaitableFuture):
            if future.cancelled():
                with self._condition:
                    task = self._submitted.pop(future, None)
                if task is not None:

                    def cancel():
                        # Not once the executor has interrupted it: an interrupt also cancelling what awaits it (the
                        # main task's TaskGroup cancelling its children, say) mustn't cancel it twice.
                        if not (self._interrupted and task.cancelling()):
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
        return future.set_exception_if_not_done(error or self._last_transition[2].copy())

    def _log(self, level: int, msg: str, *args, **kwargs):
        # Only on the executor's thread: in a forked process, a handler's stream may be locked forever
        if self.is_current_executor() and not self._forked:
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

    # Both return whether this was the first stop, so recorded: only ever on the executor's own thread
    def _clean_stop(self, why: Literal["expired", "stopped", "main task returned"]) -> bool:
        match why:
            case "expired":
                assert len(self._calls) == 0, "Invariant violation: expiry with calls in flight"
                reason = "(idle) expired"
            case "stopped":
                reason = "(waiting for running jobs) manually stopped" if len(self._calls) > 0 else "(idle) manually stopped"
            case "main task returned":
                reason = f"({'tasks cancelled' if len(self._calls) > 0 else 'idle'}) main task returned"
        return self._try_transition(self.Stopping, CleanStop(reason))

    def _unclean_stop(self, reason: str, cause: BaseException | None = None) -> bool:
        why = EventLoopExecutorStopped(f"({'tasks cancelled' if len(self._calls) > 0 else 'idle'}) {reason}", cause)
        return self._try_transition(self.Stopping, why)

    def _after_fork_in_child(self):
        """
        In the child process of a fork, EventLoopExecutor instances are invalidated. This is true even if code running
        on an EventLoopExecutor's thread did the fork; asyncio's at-fork semantics are cloudy enough that it's not worth
        trying to preserve running executors' states after fork--and that's without getting into what happens when the
        parent and child both try to interact with an executor!

        When fork occurs, this method ensures the following is true in the child process for every EventLoopExecutor
        that existed when fork(2) occurred:
        - The executor gets a fresh lock (in case another thread was using it: dispatching a @bridge call, say, at the
          moment of fork(2)), as its pending AwaitableFutures have fresh conditions already, from their own fork
          handler, which runs first (see _fork_safety).
        - The executor is Shutdown, going through Stopping if it hadn't stopped, with a EventLoopExecutorForked as why, unless it had
          shut down already. Its calls' futures fail with it, and its state futures resolve: neither are cancelled by the
          fork, but their callbacks don't run here, unless added with run_in_forked_child=True.
        """
        self._condition = RLock()
        forked = EventLoopExecutorForked()
        self._try_transition(self.Stopping, forked)  # Unless it had stopped
        if self._try_transition(self.Shutdown, forked):  # Unless it had shut down
            self._finalize()

    def _finalize(self):
        # Wait for any jobs that were being submitted at the moment of shutdown to finish being submitted. Because
        # the executor has stopped already everywhere this method is called, serializing here ensures that any submits
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
            # Let calls still in flight finish (or, if they were interrupted, clean up) before re-cancelling them and
            # aborting them.
            if len(self._calls) > 0:
                loop.run_until_complete(asyncio.wait(self._calls.copy()))
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
                    for task in tasks:
                        if task.cancel():
                            # Calls among them (after a crash) tell their waiters why: see _job()
                            self._interrupted.set()
                    # TODO: log the threads this abandons, per executor: asyncio only warns, with a RuntimeWarning shown
                    # once.
                    # Created before the wait below starts its clock, so its own timeout fires (and warns) first.
                    # TODO some event loops may not accept timeout for executor shutdown, deal with that
                    tasks.add(loop.create_task(loop.shutdown_default_executor(timeout=0.05)))
                    tasks.add(loop.create_task(loop.shutdown_asyncgens()))
                    loop.run_until_complete(asyncio.wait(tasks, timeout=0.05))
            finally:
                self._finalize()
                self._calls.clear()
                if not forked:  # Else a process forked from a call, whose fork handler has finished it
                    self._loop.close()  # Shuts the executor down without waiting, if it hasn't been already
                    # The very last action: shutdown()'s waiters wait for it
                    self._transition(self.Shutdown, self._last_transition[2])
                # Last, as its callbacks may call current(): the thread keeps its context, which would keep the executor alive
                _current_executor.set(None)

    def _interrupt(self, again: bool):
        # Once the executor has stopped: cancels the main task and every call in flight. Each time with `again` (for
        # shutdown(cancel_futures=True), expiry, and the main task ending of its own accord); otherwise (for a stop that
        # doesn't interrupt, once no calls are left in flight) only if the executor hasn't interrupted anything yet.
        if again or not self._interrupted:
            for task in {self._runner.result(timeout=0), *self._calls}:
                task.cancel() and self._interrupted.set()


    async def _job(self, func, args, kwargs, future: AwaitableFuture):
        """
        Runs a job on the executor's event loop. All jobs are submitted by _start().
        Only raises in the thread on a crash (e.g. a cancel race or a broken event loop); all errors from user code are
        forwarded back to the user if the user is still accepting responses (that is, the waiting future hasn't been
        cancelled or failed with a JobCancelledByExecutorShutdown).
        """

        # We're a little precious about performance in this method since it's on the hot path:
        task, calls, expiry = asyncio.current_task(), self._calls, self._expiry
        if self.state is not self.Running:
            self._fail(future)  # Refused, as the executor stopped after the call was made, but before its job started
            return
        calls.add(task)
        expiry.reset(None) # Suspend the idle timeout
        try:
            result = func(*args, **kwargs)
            if isawaitable(result):
                result = await result
        except asyncio.CancelledError as ex:
            # Check if we got a CancelledError because the executor was interrupted with shutdown(cancel_futures=True). If
            # so, this job's waiters get an error indicating where the job was cancelled, with exception-chain-context
            # about why the executor stopped.
            if self._interrupted and task.cancelling():
                error = JobCancelledByExecutorShutdown.from_cancellation(ex, self._last_transition[2].copy())
            else:
                error = ex
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
            # never retrieved only if the job swallowed the cancellation: if the result won a race with it, the cancel()
            # that forward() queues clears the finished task's _log_traceback.
            # TODO think more about this: whether, and when, users should see that error
            future.set_result(result)
        finally:  # Once the future is resolved: a drain's interrupt below may cancel the main task awaiting this call
            calls.remove(task)
            # A drain (a stop that leaves calls in flight to finish) cancels the main task only once the last has
            # ended: here. Nothing else would, unless the main task ended of its own accord, or a later shutdown()
            # interrupted.
            if len(calls) == 0:
                # Not a callback on wait_for_state(Stopping): that fires once, as the stop is recorded (begin() covers
                # there being no calls then), on whichever thread records it, rather than as the last call ends.
                if self.state is not self.Running:
                    self._interrupt(False)
                expiry.reset(self._idle_timeout)