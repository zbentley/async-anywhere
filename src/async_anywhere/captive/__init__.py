"""A captive worker: a thread with an asyncio event loop of its own, hosting one client, whose operations any thread or
event loop can call. It's a connection pool of size one, for upstreams that allow a single connection at a time, and for
clients that must stay on the loop that created them. Besides the standard library, it needs only attrs and sniffio.

Usage
-----
Construct a CaptiveWorker with what it needs, as keyword arguments: it's final, so code that uses it holds one, rather
than subclassing it. Its arguments are checked as it's constructed, and frozen. Pass parent_task, a coroutine
function (an async def, and nothing else) to connect, and run, the client, which the worker calls with itself as soon as
its loop is running (by default, it waits until a stop cancels it). The constructor returns only once
it has started, so a closure might not hold the worker yet: hence the argument. Expose the client's operations with
`worker.bridge(func)` (or as @worker.bridge), which bridges an async function, called with just its callers' arguments:
close over the worker, if it needs it. Pass idle_timeout to stop a worker that has gone that many seconds without calls
(to act as a worker stops, see State, under Lifecycle). Pass loop_factory to make the worker's event loop other than with
asyncio.new_event_loop(), as for asyncio.Runner. It MUST return a brand-new event loop, not yet
started, whose state isn't bound to the thread that calls it: the worker calls it on the thread constructing the worker,
but runs the loop on the worker's own. Loops that bind themselves to the thread making them as they're made (installing
signal handlers, say) aren't supported. uvloop.new_event_loop is fine.

Callers can use any async library that awaits a call can: asyncio (on any loop), or trio, or anyio on either. The worker
itself runs on asyncio, so bridged functions and the parent task are asyncio code, which may use anyio (whose asyncio
backend finds itself there). A parent task written for trio can run as a guest on the worker's loop, through
trio.lowlevel.start_guest_run(): see TrioGuest in tests/test_captive/test_runtimes.py for a recipe.

Bridged functions stay asyncio code, but can hand trio code to such a guest: in TrioGuest's recipe, its run() starts the
trio code in a nursery the guest keeps open (through the run's TrioToken, as trio code can't be started from asyncio
directly), and waits for it. What the trio code returns or raises is what the bridged function does, and cancelling the
job (as its caller cancels the call, or the worker interrupts it) cancels the trio code's own CancelScope, then waits for
it to clean up. Interrupting the worker cancels the guest too, and with it whatever trio code is still running there.

Lifecycle
---------
Constructing a worker starts its thread, which runs the parent task before anything else.

## State:

A worker is a WaitableStateTracker (see async_anywhere.state_tracker), whose `state` only moves forward: it's
CaptiveWorker.Running once constructed, Stopping once stopped, and Stopped once shut down (see below), as its copy is in
a process forked from the worker's. `worker.require(state, *states)` raises StateTrackerError unless the worker is in one
of them, and `worker.wait_for_state(state, *states)` returns an AwaitableFuture that resolves as the worker enters one of
them, or at once if it's in one, to (old state, new state, data): data being None for Running, and otherwise why the
worker stopped (a WorkerStopped, which every call from then on is refused with: in a forked process, a WorkerForked). It
never resolves for a state the worker passed before it was made, though: wait for (Stopping, Stopped) rather than just
Stopping. stop() returns such futures too. They can be waited on from any thread, or awaited on any loop, and neither
giving up waiting on one nor cancelling it does anything.

To act as the worker stops, or once it has shut down, add a done callback to such a future, and keep the future: the
worker holds the futures waiting for its states weakly, and never resolves dropped ones. Callbacks run on the worker's
thread (on its loop, or outside it, as it shuts down), or at once, if added once the future has resolved. As for any
AwaitableFuture, they never run in a process forked from the one that added them, unless added with
run_in_forked_child=True (see async_anywhere.futures._fork_safety). They must never block, nor do anything but brief,
constant-time work. What they raise is logged (by concurrent.futures), not raised.

## Worker stop:

Once a worker is stopped (Stopping), it will not accept new jobs. In-progress jobs and the parent task may be interrupted, or may continue running, depending on how a stop was triggered, see "Interruption" below.

After a worker has been stopped, new attempts to call any bridge methods will fail with an error.

Workers are stopped in these situations:

- A call to `stop()` is made.
- The parent task finishes.
- After a fork, workers inherited by a child process register as being stopped in that process.

## Worker shutdown:

A worker is shut down (Stopped) once it has terminated its event loop. The internal order of shutdown is roughly:

1. Ensure the worker is stopped, or stop it.
2. Ensure the parent task and all bridge jobs in flight have completed (see "Interruption" for cancellation semantics).
3. Abort all other active work on the captive event loop.
4. Stop the captive event loop and remove references to all active work.

After a worker has shut down, attempts to retrieve the result of any incomplete bridge calls initiated before the shutdown will fail with an error.

Shutdown occurs in the following situations:


- If the parent task or any bridge job raises  `SystemExit` or `KeyboardInterrupt`, a worker will be shut down.
- After the parent task finishes (via ordinary completion, expiration, or interruption),the worker is shut down.
- After a fork, workers inherited by a child process register as being shut down in that process.

## Interruption

User code is interrupted (sent a cancellation) in these situations:

The parent task:
    - The idle timeout expires.
    - `stop(interrupt=True)` is called.
    - `stop(interrupt=False)` is called, once no in-progress bridge jobs are running. This interruption will only be sent to the parent task once, and will only be sent if it has not been interrupted for any other reason.

Bridge jobs:
    - `stop(interrupt=True)` is called.
    - The parent task completes unexpectedly (returns or raises an exception without `stop()` having been called or expiration having
      occurred). This has the same effect as calling `stop(interrupt=True)`.
    - A bridge job's caller cancels it (e.g. via `worker.bridge_call().cancel()` or by triggering cancellation during
      `await worker.bridge_call(...)`).

Other tasks created inside the worker (e.g. via worker code using `asyncio.shield` or un-awaited `asyncio.Task`s):
    - User code triggers an ordinary cancellation.
    - The worker finishes shutting down.

**Note:** a cancellation is sent to the parent task and running bridge jobs *each* time `stop(interrupt=True)` is called. This can
lead to them receiving multiple cancellation events from the worker. Code which swallows or deduplicates cancellations (e.g. `TaskGroup`) may behave unexpectedly when this occurs.

**Note:** code that chooses to ignore cancellations may miss worker interrupts or expirations.

The first stop moves the worker to Stopping, with why as the data of that transition, and of those after it: a
WorkerStopped (a CleanStop for expiry, a stop() that interrupts nothing, or the parent task returning), which it logs.
Later stops don't change that, and every call from then on is refused with it. Whatever the worker raises it for, or chains it to, gets a copy, keeping its cause,
so callers' tracebacks don't pile up on one shared exception.

Once the parent task and every call have finished, the loop cancels any tasks left on it (work that the parent task or
calls spawned and didn't wait for), gives them 0.05s, shuts down async generators and the default executor (abandoning, with a RuntimeWarning,
threads that haven't finished by then), and closes. Only then does the worker move to Stopped, waking stop()'s waiters.

Invariants
----------
- Every call resolves. One the loop drops unrun as it closes fails with why the worker stopped, which stop()'s waiters
  get in its result: nothing waits forever on a worker that has finished (bar wait_for_state() for a state it had
  passed before the future was made: see State).
- Nothing outside the worker blocks on its loop. Making, waiting on and cancelling a call only queue callbacks on it, so a
  worker loop blocked by synchronous code holds callers up only as long as they wait for results.
- Bar stop(interrupt=True), which cancels each time it's called (see Interruption), and a crash's teardown, the worker
  cancels each task at most once, so cleanup can await without being cut short.
- A caller cancelling its call cancels the job, but a job's ending never cancels its caller.
- Why the worker stopped (a WorkerStopped) is kept apart from what calls raise.
- Giving up waiting on stop() never cancels the stop, nor a drain.
- Idle expiry never fires while a call is in flight. (Once the worker has stopped, it may, but then does nothing.)

One invariant is the user's to uphold: never run another event loop on a worker's thread. While the worker's loop runs,
asyncio refuses to anyway; this is about code that thread runs outside it, such as finalizers as the loop starts and
stops. The worker takes any code on its thread to be in its loop (see in_worker), so a bridged function called from another
loop there would run the client on that loop.

Calling a bridged function, compared with awaiting a coroutine
--------------------------------------------------------------
Calling a bridged function is like create_task() with an eager task factory: the job starts as soon as the worker's loop
takes the call, ahead of anything queued after it, and runs in the background, whatever the caller's await points (calls
made together run concurrently).

From any thread, it returns an AwaitableFuture: a concurrent.futures.Future, which can also be awaited on any loop. Once
the worker has stopped, it raises why instead, as Executor.submit() does once shut down.
- It can be waited on repeatedly, and from several threads at once. One nobody waits on runs all the same, but what it
  raises goes unreported (where asyncio would log a task's as never retrieved).
- Giving up a result() or exception() (as it times out, say) leaves the call running, as for any concurrent future.
  Cancelling the future, or a task awaiting it, cancels the job, without waiting for it to finish cancelling.
- Waiting on an unfinished call with result() or exception() on the worker's own loop raises RuntimeError, as nothing
  could finish it.
- But it isn't a coroutine, nor an asyncio future: create_task(), TaskGroup.create_task() and asyncio.wait() reject it, and
  inspect.iscoroutinefunction() is False for bridged functions (so frameworks may take them for synchronous). For APIs
  that take asyncio futures, use call.as_async_future(), which (unlike asyncio.wrap_future(call)) cancels the job as
  soon as it's cancelled. It can be awaited in a trio task too (anyio's, on trio, included), whose cancellation cancels
  the job, as on asyncio.
- The job runs in a task of its own on the worker's loop, with a copy of the caller's context variables as they were when
  the call was made, bar sniffio's record of the caller's async library (which anyio.run() sets): anyio, in the job,
  finds asyncio. Changes it makes to them don't reach the caller. Library state the copy carries, tied to
  the caller's thread, may or may not keep working on the worker's: for a synchronous caller (through asgiref's
  async_to_sync()), the job's thread-sensitive sync_to_async() calls run on the caller's thread with asgiref 3.11, but on
  another with 3.12.
- Its return value and exceptions reach the caller as the same objects, raised again there. So they, and the arguments,
  cross between loops and threads: they mustn't be bound to either loop (asyncio futures, tasks, streams, async
  generators, clients), and asyncio's Event, Queue and Lock aren't thread-safe.
- Exceptions differ in four cases:
  - Refused, as the worker stopped after the call was made, but before its job started: WorkerStopped.
  - Cut short by an interrupt: concurrent.futures.CancelledError (a private subclass), raised from a copy of the
    WorkerStopped, itself raised from whatever stopped the worker, if anything. Its traceback ends where the job was
    cancelled. It's not asyncio's CancelledError, which asyncio would take for the caller's own task being cancelled.
  - The function raising asyncio's CancelledError itself, without being cancelled: the caller gets that CancelledError,
    raised by its await or result(), rather than a cancelled future.
  - The caller cancelling: it gets its own cancellation, as usual.

Calls made on the worker's own thread are no different.

stop(interrupt=False, wait=True) isn't bridged. It queues the stop on the worker's loop, behind calls already made (which
a drain lets finish), from any thread, the worker's own included. It returns wait_for_state(Stopped)'s AwaitableFuture,
or with wait=False, (Stopping, Stopped)'s: resolving once the worker has finished, or once the stop has begun, to (old state,
new state, why the worker stopped), why being a WorkerStopped, which it returns rather than raises (a WorkerForked, in a
process forked from the worker's). On the worker's own thread,
wait=True raises RuntimeError, as it would wait for itself, and interrupt=True cancels the calling task as it does any
other. In a process forked from the worker's, it raises WorkerForked, as bridged functions do.

Caveats
-------
- A task that ignores cancellation holds stopping up forever if it's the parent task or a call: see Interruption.
- Tasks that the parent task and calls spawn are abandoned as the loop closes (asyncio logs "Task was destroyed but it
  is pending!"), so the parent task should wait for whatever it spawns.
- Synchronous code blocking the worker's loop delays everything on it: calls start, and cancellations land, once it yields.
- KeyboardInterrupt or SystemExit from the parent task or a call crash the loop, as asyncio lets them escape any task. The crash is
  recorded as the stop (and logged, if it's the first), and re-raised on the worker's thread, and, as a copy, by the call
  that raised it, in its caller. Other calls in flight are interrupted, with only the teardown's 0.05s to clean up.
- The constructor waits for the worker's loop to start running, briefly holding up a loop it's called on. Make the client
  in the parent task, not before constructing the worker, perhaps in a caller's loop, which a client made there would be
  bound to.
- Once the worker has stopped, bridged functions are refused even to the parent task: its cleanup must use the client
  directly. And a drain waits for calls that the parent task's own tasks have in flight, as for any other.
- anyio's task groups cut their children's cleanup short as they're cancelled, unless it's shielded: as ever with anyio.
- A worker may stop just as a call is made, by expiry for one: callers that should carry on can retry on CleanStop, from
  the call or its result, with a fresh worker. On uvloop, which runs overdue timers ahead of callbacks queued while the
  loop was blocked, a call made while synchronous code blocks the worker's loop past its idle timeout is refused so,
  where asyncio's loop would take the call first.
- uvloop's executor shutdown, unlike asyncio's, ignores its timeout, so when the worker abandons executor threads (see
  Lifecycle), it's abandoned too: asyncio logs "Task was destroyed but it is pending!" for it, and it raises
  RuntimeError("Event loop is closed") on a thread of its own, once those threads finish.
- Workers run on daemon threads: one not shut down before the interpreter exits stops without cleaning up (the parent
  task's cleanup never runs). Shut workers down with atexit if that matters.
- A process forked with workers alive inherits copies of them, but not their threads, so none can be used there: see
  _fork_safety (unless they'd shut down already, they're Stopped there, why being a WorkerForked, which is a
  WorkerBroken, itself a WorkerStopped). That goes for a call that forked too, whose
  child runs on in a copy of the worker's loop (one asyncio no longer counts as running): the worker carries on only in
  the parent.
- If you perform blocking waits on this worker when completed or interrupted code inside the worker is garbage collected
  or during handling of its exception classes, you may break this class's invariant (this is generally also true for all
  other asyncio code in Python).
- How interrupted calls reach callers that await them relies on asyncio.futures._convert_future_exc() (which
  as_async_future() and asyncio.wrap_future() use) converting exactly concurrent.futures.CancelledError to asyncio's, and
  nothing else: a private detail of asyncio.
"""
import asyncio
import logging
from contextlib import suppress
from contextvars import Context, copy_context
from functools import cached_property, wraps
from threading import Thread, current_thread, main_thread, Lock
from typing import Any, Callable, ClassVar, Coroutine, Literal, final
from weakref import WeakSet

import sniffio
from attrs import Factory, define, field, setters, validators

from async_anywhere._util import validate_coroutine_function, copy_exception
from async_anywhere.captive import _fork_safety
from async_anywhere.captive.exceptions import CleanStop, WorkerBroken, WorkerInterrupted, WorkerStopped, WorkerForked
from async_anywhere.captive._timer import Timer
from async_anywhere.futures import AwaitableFuture
from async_anywhere.state_tracker import State
from async_anywhere.state_tracker.waitable import WaitableStateTracker


async def _until_cancelled(_):
    await asyncio.Future()

def _timer_factory(worker: CaptiveWorker):
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
class CaptiveWorker(WaitableStateTracker):
    # See State, in Lifecycle. Every transition's data but the initial state's (_last_transition[2]) is why it stopped.
    Running: ClassVar[State] = State(initial=True)
    Stopping: ClassVar[State] = State(sources=Running)
    Stopped: ClassVar[State] = State(sources=Stopping)  # Through Stopping even after a fork: see _after_fork_in_child()

    # Run with the worker, on its loop, as soon as that's running: until a stop cancels it, by default. See Usage.
    parent_task: Callable[[CaptiveWorker], Coroutine[Any, Any, Any]] = field(
        default=_until_cancelled, validator=validate_coroutine_function
    )
    idle_timeout: float = field(
        default=float("inf"), validator=[validators.instance_of((int, float)), validators.gt(0)]
    )
    # Makes the worker's loop, once, as it's constructed: see _loop
    loop_factory: Callable[[], asyncio.AbstractEventLoop] = field(
        default=asyncio.new_event_loop, validator=validators.is_callable()
    )
    logger: logging.Logger = field(
        init=False,
        factory=lambda: logging.getLogger(CaptiveWorker.__qualname__),
    )

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

    @cached_property
    def _loop(self) -> asyncio.AbstractEventLoop:
        # Made by the fields that need it, as the worker's constructed, so on one thread. Run by _main() on the worker's
        # thread: until then, work is just queued on it.
        loop = self.loop_factory()
        if not isinstance(loop, asyncio.AbstractEventLoop):
            raise TypeError(f"loop_factory must make a new event loop, not {loop!r}")
        if loop.is_running() or loop.is_closed():
            raise TypeError(f"loop_factory made a running or closed event loop: {loop!r}")
        return loop

    def __attrs_post_init__(self):
        # Its futures (stop()'s, say) refuse blocking waits on its loop, as calls' do
        WaitableStateTracker.__init__(self, prevent_blocking_wait_in_loops=self._loop)
        _fork_safety.workers.ensure_registered(self)  # Before the thread starts, so the fork handler knows of every worker running
        self._thread.start()
        self._runner.result()  # So that, once constructed, the worker is running

    def bridge[**P, T](self, func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, AwaitableFuture[T]]:
        # Best-effort: it's calling bridged functions that's always checked
        if isinstance(broken := self._last_transition[2], WorkerBroken):
            raise copy_exception(broken)
        # The jobs' name, which their coroutines' and tasks' reprs show (tracebacks show the code's own, _job)
        name = f"{self._job.__qualname__}[{getattr(func, '__qualname__', None) or repr(func)}]"

        @wraps(func)
        def call(*args: P.args, **kwargs: P.kwargs) -> AwaitableFuture[T]:
            # A call's future, with its job queued for _start(). Refused (raising why it stopped) once the worker has stopped:
            # _job() would refuse it too, but without waiting for the loop is better. And so _finalize(), failing what's
            # pending once the worker has stopped, after taking _condition, can't miss anything.
            in_loop = self.in_worker()
            loop = self._loop
            future = AwaitableFuture()
            future._runtime_checker = self._runtime_checker  # Refusing blocking waits on the worker's loop, as its own do
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

        return call

    def in_worker(self) -> bool:
        """
        Returns True if called from the worker's captive event loop thread. Returns False otherwise. Also returns False
        if the worker's thread has fork(2)ed and this method is called from a child process.
        """
        # The worker's thread becomes the main thread if a fork() has occurred. Since the worker's thread is a daemon,
        # it'll never become the main thread in the parent, even if the original main thread exits.
        return current_thread() is self._thread is not main_thread()

    def stop(self, interrupt=False, wait=True) -> AwaitableFuture[WorkerStopped]:
        """Stop the worker, and return a future that resolves to why once the worker has finished, or with `wait=False`, once
        the stop has begun.

        Calls are refused from then on. Those in flight are cancelled, along with the parent task, each time this is
        called with `interrupt`, or left to finish before the parent task is (a drain, which a later interrupting stop()
        can cut short). On the
        worker's own thread, `wait` is an error, as it would wait for itself, and interrupting cancels the calling task
        too."""

        if isinstance(broken := self._last_transition[2], WorkerBroken):
            raise copy_exception(broken)
        # Eagerly, rather than leaving it to its future's own check: that catches result(), but not a call awaiting it,
        # which would deadlock, as a drain would wait for that very call.
        if wait and self.in_worker():
            raise RuntimeError("stop(wait=True) on the worker's own thread would wait for itself")

        def begin():  # On the worker's loop. Not a call, so it isn't refused once the worker has stopped: it can cut a drain short.
            if interrupt and len(self._calls) > 0:
                self._unclean_stop("manually stopped")
            else:
                self._clean_stop("stopped")
            if interrupt or len(self._calls) == 0:
                self._interrupt(interrupt)  # Otherwise the last call to end does: see _job()

        with suppress(RuntimeError):  # The loop has closed, so the worker has already stopped
            self._loop.call_soon_threadsafe(begin)  # Queued even on the worker's thread, behind calls made before it
        return self.wait_for_state(self.Stopped) if wait else self.wait_for_state(self.Stopping, self.Stopped)

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
            # Naming what recorded the stop: past this, _stop(), and _clean_stop() or _unclean_stop()
            self.logger.log(level, msg, *args, stacklevel=4, **kwargs)

    def _clean_stop(self, why: Literal["expired", "stopped", "parent task returned"]) -> bool:
        match why:
            case "expired":
                assert len(self._calls) == 0, "Invariant violation: expiry with calls in flight"
                reason = "(idle) expired"
            case "stopped":
                reason = "(waiting for running jobs) manually stopped" if len(self._calls) > 0 else "(idle) manually stopped"
            case "parent task returned":
                reason = f"({'interrupted' if len(self._calls) > 0 else 'idle'}) parent task returned"
        return self._stop(CleanStop(reason))

    def _unclean_stop(self, reason: str, cause: BaseException | None = None) -> bool:
        return self._stop(WorkerStopped(f"({'interrupted' if len(self._calls) > 0 else 'idle'}) {reason}", cause))

    def _stop(self, why: WorkerStopped) -> bool:
        # Whether this was the first stop, so recorded: only ever on the worker's own thread
        if not self._try_transition(self.Stopping, why):
            return False
        if isinstance(why, CleanStop):
            self._log(logging.INFO, why.args[0])
        else:
            self._log(logging.ERROR, "Stopping due to error:", exc_info=why)
        return True

    def _after_fork_in_child(self):
        """
        In the child process of a fork, CaptiveWorker instances are invalidated. This is true even if code running on a
        CaptiveWorker's thread did the fork; asyncio's at-fork semantics are cloudy enough that it's not worth trying to
        preserve running workers' states after fork--and that's without getting into what happens when the parent and
        child both try to interact with a worker!

        When fork occurs, this method ensures the following is true in the child process for every CaptiveWorker that
        existed when fork(2) occurred:
        - The worker gets a fresh lock (in case another thread was using it: dispatching a @bridge call, say, at the
          moment of fork(2)), as its pending AwaitableFutures have fresh conditions already, from their own fork
          handler, which runs first (see _fork_safety).
        - The worker is Stopped, going through Stopping if it hadn't stopped, with a WorkerForked as why, unless it had
          shut down already. Its calls' futures fail with it, and its state futures resolve: neither are cancelled by the
          fork, but their callbacks don't run here, unless added with run_in_forked_child=True.
        """
        self._condition = Lock()
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
            self._expiry.reset(self.idle_timeout)  # "Reset that clock!" - Idris Elba
            self._runner.set_result(asyncio.current_task())
            await self.parent_task(self)

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
                # The parent task ending was the stop, so it interrupts calls, as stop(interrupt=True) would
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
                    # The very last action: stop()'s waiters wait for it
                    self._transition(self.Stopped, self._last_transition[2])

    def _interrupt(self, again: bool):
        # Once the worker has stopped: cancels the parent task and every call in flight. Each time with `again` (for
        # stop(interrupt=True), expiry, and the parent task ending of its own accord); otherwise (for a stop that
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
            # Check if we got a CancelledError because the worker was interrupted with stop(interrupt=True). If so, this
            # job's waiters get an error indicating where the job was cancelled, with exception-chain-context about why
            # the worker stopped.
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
            # ended: here. Nothing else would, unless the parent task ended of its own accord, or a later stop()
            # interrupted.
            if len(calls) == 0:
                # Not a callback on wait_for_state(Stopping): that fires once, as the stop is recorded (begin() covers
                # there being no calls then), on whichever thread records it, rather than as the last call ends.
                if self.state is not self.Running:
                    self._interrupt(False)
                expiry.reset(self.idle_timeout)
