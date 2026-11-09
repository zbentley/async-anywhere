import asyncio
import concurrent.futures
from concurrent.futures._base import CANCELLED_AND_NOTIFIED, FINISHED, PENDING, RUNNING
from math import isinf
from threading import Condition, Thread
from time import monotonic
from typing import Any, Awaitable, Callable, Self, Collection

from async_anywhere._util import AnyEventLoop, BlockingChecker, BlockingNotAllowed, futures_logger
from async_anywhere._util.trio import TrioRuntime
from async_anywhere.futures import _fork_safety
from async_anywhere.futures._asyncio import AsyncioAwaitableFuture
from async_anywhere.futures._trio import TrioAwaitableFuture
from async_anywhere.state_tracker import State, StateTracker


class _DoneCallbackToken:
    __slots__ = ()


class _ForkAllowedDoneCallbackToken(_DoneCallbackToken):
    __slots__ = ()


class AwaitableFuture[T](concurrent.futures.Future[T], StateTracker, Awaitable[T]):
    r"""A future that can be waited on by synchronous *or* asynchronous code; acts like a hybrid of
    :py:obj:`concurrent.futures.Future` and :py:obj:`asyncio.Future`.

    Provides the following behaviors:

    - Calling ``await AwaitableFuture(...)`` can be done from any thread, async event loop context, or :doc:`Trio <trio:index>` task.
    - Synchronous waits with an optional timeout can be performed via :py:meth:`result` or
      :py:meth:`exception`.
    - Any number of event loops, async tasks, or threads can wait on an ``AwaitableFuture`` simultaneously.
    - Users can specify threads or event loops upon which blocking waits are not allowed. This turns a common cause
      of deadlocks (erroneously synchronously waiting for an incomplete future on the loop which was intended to
      complete it) into an error. See the ``prevent_blocking_wait_when`` constructor parameter for more information.
    - Unified and configurable cancellation behavior across asynchronous and synchronous waiters. This
      improves on the underspecified and inconsistent semantics regarding cancellation of
      linked :py:obj:`concurrent.futures.Future`\ s and :py:obj:`asyncio.Future`\ s.
    - Significantly improved performance and reduced locking compared to :py:obj:`concurrent.futures.Future`.
    - Fork-safety: documented and configurable semantics regarding the behavior of incomplete ``AwaitableFuture``
      objects in the presence of :py:func:`os.fork`; see :ref:`future_fork_safety`.
    - :term:`Completing` an ``AwaitableFuture`` on a running event loop's thread also completes any
      :py:obj:`asyncio.Future`-like objects that are linked to it at once, rather than in a later loop tick (which is
      what :py:func:`asyncio.wrap_future` does).
    - Several other convenience methods to save code for common future-related tasks (including lock-free accessors and
      mutators, improved tools for observing future state, and more).

    """

    # Running is as an executor's job is once started, which can't be cancelled. Once done, it never transitions again.
    Pending = State(initial=True)
    Running = State(sources=Pending)
    Cancelled = State(sources=(Pending, Running))  # From Running only in a forked process: see _fork_safety
    Succeeded = State(sources=(Pending, Running))  # Its transition's data is the result
    Failed = State(sources=(Pending, Running))  # Its transition's data is the exception
    # See _state. Cancelling notifies concurrent.futures' waiters at once, so it's never just CANCELLED.
    _CF_STATES = {
        Pending: PENDING, Running: RUNNING, Cancelled: CANCELLED_AND_NOTIFIED, Succeeded: FINISHED, Failed: FINISHED
    }
    # For prevent_blocking_wait_when: refusing blocking waits in any running event loop or trio task, or anywhere
    AnyEventLoop = staticmethod(AnyEventLoop)
    BlockingNotAllowed = staticmethod(BlockingNotAllowed)

    __slots__ = (
        '_blocking_checker',  # Refuses blocking waits where they'd deadlock (see BlockingChecker), or None
        '_ignore_cancel',
        '_cancel_pending_in_forked_child',
        '_transform_result',
        '_completed_at',
        # Its done callbacks by token, in the order added (those with a ForkAllowedDoneCallbackToken run in a forked
        # process), dropped once they've run, and concurrent.futures.wait()'s and as_completed()'s waiters: both changed
        # only under _condition
        '_done_callbacks',
        '_waiters',
    )
    def __init__(
        self,
        *,
        # TODO fix doc type hint
        prevent_blocking_wait_when: Collection[asyncio.AbstractEventLoop | Thread] | None = None,
        ignore_cancel: bool = False,
        cancel_pending_in_forked_child: bool = False,
        transform_result: Callable[[Any], T] | None = None,
    ):
        r"""
        Cancelling ``await``\ s on this class or on :py:obj:`asyncio.Future`-like objects produced by it
        *immediately* cancels the underlying :py:obj:`concurrent.futures.Future` (if cancellation is allowed; see
        ``ignore_cancel`` below), rather than postponing cancellation until a later event loop iteration as the
        stdlib does.

        Args:
            prevent_blocking_wait_when: a Collection of :py:obj:`asyncio.AbstractEventLoop`\ s or
                :py:obj:`threading.Thread`\ s, or None. When a collection is passed, this future refuses to be blocked
                on from callers in it: when :py:meth:`result` is called from one of those loops or threads and this
                future is incomplete, an error will be raised instead of allowing ``.result()`` to synchronously block.

                This is provided purely for convenience, since "accidental blocking waits in an event loop" is a very
                easy defect to accidentally introduce.

                Only weak references to supplied event loops and threads are tracked internally.

                If None or an empty collection is passed, no blocking-allowed checks are performed.
            ignore_cancel: If True, calls to :py:meth:`cancel` this future or its descendants from asynchronous or
                synchronous code will return False and the future will not enter the cancelled state. If False,
                cancellation behavior is the same as the behavior of futures in the stdlib.

                :py:obj:`asyncio.Future`\ s can be :py:func:`asyncio.shield`\ ed to deflect cancellations, but
                :py:obj:`concurrent.futures.Future`\ s have no equivalent functionality. This parameter unifies that
                behavior: ``AwaitableFuture``\ s with ``ignore_cancel=True`` behave as though ``asyncio.shield``\ ed
                when waited upon.

                Note:
                    ``ignore_cancel=True``\ 's behavior may cause unexpected side effects:

                    - Some code in the stdlib that waits on futures (like :py:func:`concurrent.futures.wait`,
                      :py:func:`asyncio.wait`, and :py:func:`asyncio.gather`) will do the "correct" thing and, after
                      calling ``.cancel()`` on a future, wait for it to complete. With ``ignore_cancel=True`` set, those
                      functions will then wait forever (or until the ``AwaitableFuture`` is completed some other way).
                    - Some *other* code in the stdlib (e.g. :py:obj:`asyncio.TaskGroup`, :py:func:`asyncio.wait`, and
                      :py:func:`asyncio.timeout`) internally repeatedly converts arguments to `asyncio.Future`\ s and
                      attaches callbacks to them. If passed an ``AwaitableFuture`` with ``ignore_cancel=True``, such
                      functions may internally create a large number of never-completed transient wrapper futures,
                      which will take up memory, and will lead to many spurious "exception was never retrieved" logs
                      when the wrapping/waiting function finally completes. If you encounter this behavior or need to
                      use troublesome stdlib future-wrapping functions with ``ignore_cancel=True``, pass the result of
                      :py:meth:`as_async_future` to those functions directly.

            cancel_pending_in_forked_child: if True, this future will automatically be cancelled in any child
                process created by :py:func:`os.fork` if it is not yet :term:`completed`. If False, this future can
                still be safely waited on after a fork, in the child process (though it may never complete, depending on
                what you are using it for). See :ref:`future_fork_safety`.
            transform_result: a callable which will be called with the arguments of :py:meth:`set_result`, whenever
                ``set_result`` is called. The value returned from that call will be stored as the future's result.

                Warning:
                    ``transform_result`` may be called:

                    - From any thread, process, or event loop.
                    - Any number of times for the same result value.
                    - Even if a result cannot be successfully set (e.g. if the future is already completed).

                    If ``transform_result`` raises an error,  :py:meth:`set_result` will fail and
                    :py:meth:`set_exception` will be called with the error instead.

                    Because of this, code in ``transform_result`` must never block, should perform as few OS
                    interactions as possible, should avoid raising exceptions, and should ideally be as close to a pure
                    (and pure Python) constant-time function as possible.

        """
        # NB: we're deliberately not calling concurrent.future.Future's super().__init__. We replace basically all of
        # that class's internals, and subclass it only to satisfy isinstance() checks performed by stdlib. This class's
        # observable behavior contract (which, per the stdlib's documented warning, is NOT limited to public methods)
        # must remain a superset of concurrent.future.Future's contract.
        StateTracker.__init__(self, lock=Condition())
        # Not making a BlockingChecker for None (most futures) saves its call: these are made on hot paths
        # Or a checker made already, passed in, as an executor's are
        if prevent_blocking_wait_when is None or isinstance(prevent_blocking_wait_when, BlockingChecker):
            self._blocking_checker = prevent_blocking_wait_when
        elif len(prevent_blocking_wait_when) == 0:
            self._blocking_checker = None
        else:
            self._blocking_checker = BlockingChecker(prevent_blocking_wait_when)
        self._ignore_cancel = ignore_cancel
        self._cancel_pending_in_forked_child = cancel_pending_in_forked_child
        self._transform_result = transform_result
        self._completed_at: float | None = None
        self._done_callbacks: dict[_DoneCallbackToken, Callable[[Self], Any]] = {}

        # Mutated only by code in concurrent.futures.wait() and .as_completed(). Contains opaque objects installed and
        # removed by those methods.
        self._waiters: list = []
        _fork_safety.futures.ensure_registered(self)  # Until it has finished

    def __repr__(self):
        # Unlike concurrent.futures.Future, we don't need to take the lock for repr, since reading the field is atomic.
        # Our repr is similar to, but not the same as, concurrent.futures.Future's repr.
        _, state, data = self._last_transition
        outcome = "" if data is None else f"[{type(data).__name__}]"
        return f"<{type(self).__qualname__} at {id(self):#x} {state.name}{outcome}>"

    def _after_state_change(self, old_state: State, new_state: State, data: Any):
        # Under _condition, so concurrent.futures.wait() and as_completed(), which add their waiters under it, miss nothing
        if new_state is self.Running:
            return
        self._completed_at = monotonic()
        for waiter in self._waiters:
            if new_state is self.Succeeded:
                waiter.add_result(self)
            elif new_state is self.Failed:
                waiter.add_exception(self)
            else:
                waiter.add_cancelled(self)
        self._condition.notify_all()

    @property
    def completed_at(self) -> float | None:
        """Returns the timestamp at which this future was completed, or None if it has not yet completed.

        The timestamp returned is a monotonic floating point value that does not necessarily correspond to the system
        time; see :py:func:`time.monotonic` for details.

        Takes out no locks if this future has already completed.

        Note:
            This function does *not* return the timestamp according to the event loop (if any) that was running when
            a future was completed. However, many/most implementations of :py:meth:`AbstractEventLoop.time <asyncio.loop.time>` return
            values from the same monotonic clock used by this class, so the two values are often safe to compare.
        """
        # Optimistically avoid the lock, since it only gets set once.
        if (completed_at := self._completed_at) is not None:
            return completed_at
        with self._condition:  # Set just after it finishes, under this: so it's set for anyone who has seen it done
            return self._completed_at

    def as_async_future(
        self, loop: asyncio.AbstractEventLoop | None = None, require_running_loop=True
    ) -> asyncio.Future[T] | Awaitable[T]:
        r"""
        Returns an :py:obj:`asyncio.Future` or :doc:`Trio <trio:index>` awaitable linked to the invocant such that it
        can be waited on for the invocant's result, or cancelled such that both it and the invocant are cancelled.

        For advanced use-cases only; most code can just ``await AwaitableFuture(...)`` and get the same behavior.

        Use this method if your code needs to handle native asyncio futures, or objects for which
        :py:func:`asyncio.isfuture` is true. While ``AwaitableFuture`` is, as the name suggests, *awaitable*, it is not
        recognized by ``asyncio`` as an async *Future*, just an awaitable. This method may be useful in cases like:

        - You want to pass an ``AwaitableFuture`` to stdlib methods that themselves automatically wrap things that are
          not ``asyncio.Future``\ s in ``asyncio.Future``\ s, but don't want wrapping to reoccur each time you pass
          this class into those functions (for example, you might want to rely on the identity of objects returned by
          functions like :py:func:``asyncio.wait``).
        - You want to pre-construct an ``asyncio.Future`` attached to a different event loop elsewhere in your program
          (that may or may not be running).
        - You want to maximize performance of ``await``\ ing on completed ``AwaitableFuture``\ s on a specific event
          loop (this method is slightly faster than regular ``await AwaitableFuture(..)``, but not much).

        Args:
            loop: The event loop to which the returned :py:obj:`asyncio.Future` should be bound, or None. If None,
                either Trio will be used (if it is running) or the current asyncio event loop will be used.
            require_running_loop: If ``loop`` is supplied, or if ``loop`` is None and Trio is not active, the event loop
                used for the returned ``asyncio.Future`` must be running, otherwise an error will be raised by this
                method. Just for convenience/runtime bug-checking, since it's easy in async Python to attach futures to
                the wrong event loop or stopped event loops.

        Raises:
            :py:obj:`RuntimeError`: if ``require_running_loop=True`` and ``loop`` (or the current event loop, if Trio
                is not active) is not running.

        Returns:
            If ``loop`` is a non-Trio event loop or a non-Trio event loop is running, returns an
            :py:obj:`asyncio.Future` (*or subclass thereof*) that is linked to this ``AwaitableFuture`` and
            attached to the specified event loop. Cancelling either future cancels the other.

            Note:
                The returned :py:obj:`asyncio.Future` will does *not* support calls to ``set_result`` or
                ``set_exception`` attempting to call either method will fail with an error; to set the result or
                exception of the returned future, call :py:meth:`set_result` or :py:meth:`set_exception` on *this*
                future instead.

            If ``loop`` is None and Trio is running, returns an awaitable which can (only) be ``await``ed by the current
            Trio context. The only additional public method on the Trio-compatible awaitable is ``.cancel()``, which
            works like ``asyncio.Future.cancel`` and cancels the returned future-alike and this ``AwaitableFuture``.
            Trio-returned futures cannot have a result or exception manually set.

            Either return type, when awaited, produces the result, cancellation, or exception with which its "parent"
            ``AwaitableFuture`` was completed.

            Note:
                The returned future may not be new/unique; use :py:func:`asyncio.wrap_future` on the result of this
                method if your code requires a unique future.
        """
        if loop is None:
            if TrioRuntime.in_task(): # TODO remove dot lookup
                return TrioAwaitableFuture(self, TrioRuntime.current_token())
            loop = asyncio.get_running_loop() if require_running_loop else asyncio.get_event_loop()
        elif require_running_loop and loop.is_closed():
            raise RuntimeError('Event loop is closed')
        return AsyncioAwaitableFuture(self, loop)

    def __await__(self):
        # If we're already done, __await__ can resolve without dealing with any loop machinery at all
        if self._last_transition[1].terminal:
            return self._await_finished()
        # Not a generator, so that what awaits it awaits the asyncio.Future's own iterator directly
        return self.as_async_future().__await__()

    def _await_finished(self):
        # In trio, waiters get result()'s concurrent.futures.CancelledError, as TrioAwaitableFuture's do
        if self.cancelled() and not TrioRuntime.in_task():
            # Since result() raises concurrent.futures.CancelledError, convert to asyncio.CancelledError here
            raise asyncio.CancelledError
        return self.result()
        yield  # A generator, for __await__ to return, that finishes at once

    def cancel(self) -> bool:
        """
        Attempts to cancel this future.

        Takes out no locks if this future has already completed.

        Returns:
            True if the cancellation was recieved/accepted, False otherwise (e.g. if the future has already
            :term:`completed` or if ``ignore_cancel`` is True).
        """

        # Lock-free unless it's pending, as Executor.map() cancels every future once it has its result
        if not self._ignore_cancel and self.state is self.Pending and self._cancel():
            return True
        return self.cancelled()

    def _cancel(self, even_running: bool = False) -> bool:
        # Running can become Cancelled, but only the fork handler wants that (see _fork_safety): so cancel() rechecks
        # under _condition, reentrant, as _transition() takes it too. Callbacks run without it.
        with self._condition:
            # It started, finished, or was cancelled, meanwhile
            if not (even_running or self.state is self.Pending) or not self._try_transition(self.Cancelled):
                return False
        self._invoke_callbacks()
        return True

    def _get_result(self, expect_success: bool, timeout: float | None):
        try:
            _, state, data = self._last_transition
            match state:
                case self.Succeeded if expect_success: return data
                case self.Succeeded: return None
                case self.Failed if expect_success: raise data
                case self.Failed: return data
                case self.Cancelled: raise concurrent.futures.CancelledError
            if self._blocking_checker is not None and (timeout is None or timeout > 0):  # Else it only polls
                self._blocking_checker.check(self)
            # Convert positive infinity to None/wait-forever
            if timeout is not None and timeout > 0 and isinf(timeout):
                timeout = None
            with self._condition:
                if not self._condition.wait_for(self.done, timeout):
                    raise TimeoutError
            return self._get_result(expect_success, None)
        finally:
            del self, data

    def result(self, timeout: float | None = None) -> T:
        """
        Synchronously waits for this future's result, optionally with a timeout, like
        :py:meth:`concurrent.futures.Future.result` but with some additional behavior.

        If this future has not yet :term:`completed` when this method is called, one of several things happens:

        #. If timeout is zero, :py:obj:`TimeoutError` is raised.
        #. If any of the threads or event loops passed to the ``prevent_blocking_wait_when`` constructor parameter is
           the thread or event loop running in the caller of this method, :py:class:`.WouldBlock` is raised.
        #. Otherwise, waits for ``timeout`` seconds for this future to complete. If it succeeds, its result is returned.
           If it fails with an exception or is cancelled, that error is raised. If it doesn't complete in time,
           :py:obj:`TimeoutError` is raised.

        Args:
            timeout: Positive floating point number of seconds to wait for this future to complete, or None. If
                ``timeout`` is None or positive infinity, waits forever. If ``timeout`` is zero, this method runs in
                constant time.

        Raises:
            :py:obj:`TimeoutError`: if waiting for completion times out.

            :py:class:`WouldBlock`: if this future is incomplete *and* blocking is not allowed (per the
                ``prevent_blocking_wait_when`` constructor parameter) on the calling event loop or thread *and*
                ``timeout`` is greater than zero.

            :py:obj:`concurrent.futures.CancelledError`: if this future is cancelled before or during this method.

            :py:obj:`BaseException`: if this future was completed as failed via :py:meth:`set_exception`.

        Returns:
            The value with which this future was completed via :py:meth:`set_result` if it doesn't fail,
            isn't cancelled, and waiting for it to complete does not time out.
        """
        try:
            return self._get_result(True, timeout)
        finally:
            del self

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """
        Works just like :py:meth:`result`, but returns the exception with which this future was
        :term:`completed`, if any.

        Raises:
            :py:obj:`TimeoutError`: if waiting for completion times out.

            :py:class:`WouldBlock`: if this future is incomplete  *and* blocking is not allowed (per the
                ``prevent_blocking_wait_when`` constructor parameter) on the calling event loop or thread *and*
                ``timeout`` is greater than zero.

            :py:obj:`concurrent.futures.CancelledError`: if this future is cancelled before or during this method.

        Returns:
            The value with which this future was completed via :py:meth:`set_exception` if it
            isn't cancelled, and waiting for it to complete does not time out. Returns None if this future is completed
            via :py:meth:`set_result` instead of :py:meth:`set_exception`.

        """
        try:
            return self._get_result(False, timeout)
        finally:
            del self

    def add_done_callback(
        self, fn: Callable[[Self], Any], run_in_forked_child: bool = False, raise_if_failed_immediately=False,
    ) -> object:
        r"""
        Adds a callback which will be called when this future :term:`complete`\ s, and which is called immediately, here,
        if it already has. Like :py:meth:`concurrent.futures.Future.add_done_callback`, but with some additional
        behavior: :ref:`fork awareness <future_fork_safety>`, identifier return tokens, and improved exception semantics.

        Args:
            fn: The function to be called. Must accept one argument, which will always be the invocant.

                Warning:
                    In ``async_anywhere`` and in ordinary ``concurrent.futures`` code, done callbacks should follow the
                    below rules. If these rules are broken, unexpected and damaging behavior might occur, depending on
                    context:

                    - Done callbacks should not unduly block for more than a short, ideally constant, time.
                    - Done callbacks should make *zero* assumptions about which thread or event loop they will be called
                      from, and should assume that they can and will be called from unexpected contexts.
                    - Done callbacks should never rase a :py:obj:`BaseException` that isn't a :py:obj:`Exception`:
                      in ``concurrent.futures`` and in this code, doing so may abort and permanently orphan/never run
                      all other callbacks attached to a given future. Numerous stdlib components may malfunction after
                      e.g. :py:obj:`SystemExit` is unexpectedly raised from a done callback. In the author's
                      experience, :py:obj:`KeyboardInterrupt` delivery during done callback execution (a scenario
                      whose likelihood increases proportionally to the runtime duration of done callbacks in use) is
                      highly likely to (potentially permanently/interpreter-wide) corrupt ``concurrent.futures.Future``
                      and ``concurrent.futures.Executor`` states.


            run_in_forked_child: If True, this done callback will run in a child (or grandchild) process
                :py:func:`os.fork`\ ed off of the one that created this ``AwaitableFuture``. Depending on how this
                future is used across processes, and on the value of the ``cancel_pending_in_forked_child`` constructor
                parameter, this may cause a done callback to run more than once (which doesn't mean "twice"!).
                Defaults to False. See :ref:`future_fork_safety`.
            raise_if_failed_immediately: When done callbacks on ``AwaitableFuture``\ s and
                ``concurrent.futures.Future``\ s raise :py:obj:`Exception`\ s, those exceptions are not raised to
                any calling code and are logged to the console. If this parameter is True and ``add_done_callback`` is
                being called on an already :term:`completed` ``AwaitableFuture``, any errors raised by the supplied
                done callback will be raised, rather than logged. Defaults to False.

        Raises:
            :py:obj:`Exception`: if this future has completed *and* ``raise_if_failed_immediately=True`` *and* ``fn``
                raises an ``Exception`` when this method calls it.

            :py:obj:`BaseException`: if this future has completed *and* ``fn`` raises a non-``Exception``
                ``BaseException`` when this method calls it.

        Returns:
            An opaque token which can be passed to :py:meth:`remove_done_callback` to remove ``fn`` from
            the future's scheduled callbacks.
        """
        token = _ForkAllowedDoneCallbackToken() if run_in_forked_child else _DoneCallbackToken()
        with self._condition:
            if not self._last_transition[1].terminal:
                self._done_callbacks[token] = fn
                return token
        try:
            fn(self)
        except Exception:
            if raise_if_failed_immediately:
                raise
            futures_logger().exception("exception calling callback for %r", self)
        return token

    def remove_done_callback(
        self, fn_or_token: Callable[[Self], Any] | object
    ) -> int:
        r"""
        Unschedules a callback added via :py:meth:`add_done_callback` from the set of functions to be
        run when this future :term:`complete`\ s.

        Args:
            fn_or_token: If given the return value from an invocation of :py:meth:`add_done_callback`,
                the callback scheduled by that invocation will be removed, if it exists. If given a function/callable,
                *all* instances of that callable scheduled as done callbacks will be removed.

        Returns:
            A non-negative integer representing the number of callbacks removed.

            Note:
                If a positive value is returned by this function, it means that the specified callback *did not and
                will not* run. However, the converse isn't true: if this function returns 0, that might
                mean that the callback already ran; it might mean that the callback wasn't found; and it might mean
                that the callback is *about* to run, because this method races with the removal of callbacks that
                occurs when a future is completed.
        """

        if self._last_transition[1].terminal:
            return 0
        keep = []
        with self._condition:
            # Only while pending: once it's finished, _invoke_callbacks() takes them without _condition
            if self._last_transition[1].terminal:
                return 0
            if isinstance(fn_or_token, _DoneCallbackToken):
                # Keep the reference alive until we leave the condition, just in case a destructor runs that'd deadlock
                # with us (or, if it runs on this thread, corrupt state). The other path doesn't need this guard since
                # a strong reference to the function must have been supplied.
                keep.append(self._done_callbacks.pop(fn_or_token, None))
                return 0 if keep[0] is None else 1
            # `==`, not `is`: a bound method is a new object each time it's looked up
            tokens = [token for token, fn in self._done_callbacks.items() if fn == fn_or_token]
            for token in tokens:
                del self._done_callbacks[token]
            return len(tokens)

    # NB: named to match the method on concurrent.futures.Future, even though this class never calls into that
    # superclass's code, it's a useful belt-and-suspenders way to intercept any stdlib/other code that wants to call
    # _private methods on concurrent Futures (see also: AwaitableFuture._state).
    def _invoke_callbacks(self):
        # Not under _condition, so that a fork never leaves the copy of a pending future's held: see _fork_safety
        _fork_safety.futures.ensure_unregistered(self)
        callbacks, self._done_callbacks = self._done_callbacks, {}
        for fn in callbacks.values():
            try:
                fn(self)
            except Exception:
                futures_logger().exception("exception calling callback for %r", self)

    def _after_fork_in_child(self):
        # See _fork_safety: the threads that held its condition, or waited in concurrent.futures.wait(), aren't here
        self._condition = Condition()
        self._waiters.clear()
        callbacks = [k for k in self._done_callbacks.keys() if not isinstance(k, _ForkAllowedDoneCallbackToken)]
        for tok in callbacks:
            del self._done_callbacks[tok]

    def set_result(self, result: T):
        """
        Attempts to set the successful result of this future, :term:`completing` it and any futures derived from it.

        If the ``transform_result`` constructor parameter was passed, that function will be run on the supplied value
        before storing it. If that function raises an error, that error will be set on this future via
        :py:meth:`set_exception` and will then be raised to the caller.

        Args:
            result: The result value to store.

        Raises:
            :py:class:`StateTrackerError`: if the future has already been completed elsewhere. Note that
                ``StateTrackerError`` subclasses :py:obj:`asyncio.InvalidStateError` and
                :py:obj:`concurrent.futures.InvalidStateError` for compatibility.

            :py:obj:`BaseException`: If this future is not done *and* ``transform_result`` is set *and* the
                ``transform_result`` callable raises an error.

        Returns:
            Not meaningful/reserved for future use.
        """
        if self._transform_result is not None:
            try:
                result = self._transform_result(result)
            except BaseException as ex:
                self.set_exception(ex)
                del self, result  # So the traceback of what this raises, now its exception, holds neither
                raise
        self._transition(self.Succeeded, result)
        self._invoke_callbacks()

    def set_exception(self, exception: BaseException):
        r"""
        Works just like :py:meth:`set_result`, but stores an exception and :term:`complete`\ s this future as "failed
        with an error".

        Warning:
            Storing non-:py:obj:`Exception` :py:obj:`BaseException`\ s is dangerous and not recommended. This can
            cause problems, since many ``BaseException``\ s change interpreter behavior when they are raised (e.g.
            :py:obj:`asyncio.CancelledError` or :py:obj:`SystemExit`), and can, once stored, be raised from
            unexpected places in code that calls :py:meth:`result`.

        Args:
            exception: The exception to store.

        Raises:
            :py:class:`StateTrackerError`: if the future has already been completed elsewhere. Note that
                ``StateTrackerError`` subclasses :py:obj:`asyncio.InvalidStateError` and
                :py:obj:`concurrent.futures.InvalidStateError` for compatibility.

        Returns:
            Not meaningful/reserved for future use.

        """
        self._transition(self.Failed, exception)
        self._invoke_callbacks()

    def raise_if_failed(self):
        """If this future was :term:`completed` with an error via :py:meth:`set_exception`, raise that error.

        If this future was cancelled, raise CancelledError.

        Otherwise, does nothing.

        Takes out no locks.
        """
        if error := self.exception_or_none():
            raise error
        if self.state is self.Cancelled:
            raise concurrent.futures.CancelledError

    def set_result_if_not_done(self, result: T) -> bool:
        r"""If this future is not done, calls :py:meth:`set_result` with the supplied value.

        Takes out no locks if the future has already completed.

        Args:
            result: See :py:meth:`set_result`.

        Returns:
            True if ``result`` was stored via ``set_result``. False if the future was :term:`completed` from elsewhere.

        Raises:
            :py:obj:`BaseException`: Any exception raised by ``transform_result``, if it was passed to the constructor.


        """
        if self._last_transition[1].terminal:  # Lock-free, so cheaper than the raise below
            return False
        try:
            self.set_result(result)
            return True
        except concurrent.futures.InvalidStateError:
            return False
        except BaseException:
            del self, result  # As in set_result(): what that raised may be this future's exception
            raise

    def set_exception_if_not_done(self, exception: BaseException) -> bool:
        r"""If this future is not done, calls :py:meth:`set_exception` with the supplied value.

        Takes out no locks if the future has already completed.

        Args:
            exception: See :py:meth:`set_exception`.

        Returns:
            True if ``exception`` was stored via ``set_exception``. False if the future was :term:`completed` from elsewhere.
        """
        if self._last_transition[1].terminal:
            return False
        try:
            self.set_exception(exception)
            return True
        except concurrent.futures.InvalidStateError:
            return False

    def exception_or_none(self) -> BaseException | None:
        """Returns this future's exception if it failed with an error.

        Returns None if the future is not done, was cancelled, or :term:`completed` without an exception.

        Takes out no locks.
        """
        _, state, data = self._last_transition
        return data if state is self.Failed else None

    def result_or_none(self) -> T | None:
        """Returns the result of this future if it is done.

        Returns None if the future is not done, failed, or was cancelled.

        Takes out no locks.

        Note:
            If your future can be successfully :term:`completed` with the value ``None``, this method is likely not
            useful (use :py:meth:`result()` with ``timeout=0`` and handle the error instead).
        """
        _, state, data = self._last_transition
        return data if state is self.Succeeded else None

    def done(self) -> bool:
        """Returns True if this future has :term:`completed`, False otherwise.

        Takes out no locks.
        """
        return self._last_transition[1].terminal

    def cancelled(self) -> bool:
        """Returns True if this future was :term:`completed` by being cancelled False otherwise.

        Takes out no locks.
        """
        return self._last_transition[1] is self.Cancelled

    # For Executor API compliance
    @property
    def _state(self) -> str:
        # What concurrent.futures.wait() and as_completed() read, under _condition, on every future they're given
        return self._CF_STATES[self.state]

    # For Executor API compliance
    def running(self) -> bool:
        """
        Returns True if this future has *not* :term:`completed`, False otherwise.

        Takes out no locks.

        :meta private:

        Warning:
            This method exists to satisfy part of the concurrent.futures.Executor/as_completed/wait API and should NOT
            be called by user code.
        """
        return self.state is self.Running

    # For Executor API compliance
    def set_running_or_notify_cancel(self) -> bool:
        """
        See :py:meth:`concurrent.futures.Future.set_running_or_notify_cancel`.

        This function should generally not be called by user code, and is intended for stdlib-only internal use.
        """
        # cancel() has notified concurrent.futures' waiters already: so this just refuses a cancelled future, each time
        with self._condition:
            if self.cancelled():
                return False
            self._transition(self.Running)  # Refused once it's running or finished
        return True
