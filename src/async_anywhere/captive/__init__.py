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
close over the worker, if it needs it. Pass idle_timeout to stop a worker that has gone that many seconds without calls,
and on_stopped and on_shutdown to act as the worker stops, and once it has finished: each is called once, with the
worker, on its thread, and must never block (by default, they do nothing). Pass loop_factory to make the worker's event
loop other than with asyncio.new_event_loop(), as for asyncio.Runner. It MUST return a brand-new event loop, not yet
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

## Worker stop:

Once a worker is stopped, it will not accept new jobs. In-progress jobs and the parent task may be interrupted, or may continue running, depending on how a stop was triggered, see "Interruption" below.

After a worker has been stopped, new attempts to call any bridge methods will fail with an error.

Workers are stopped in these situations:

- A call to `stop()` is made.
- The parent task finishes.
- After a fork, workers inherited by a child process register as being stopped in that process.

## Worker shutdown:

A worker is shut down once it has terminated its event loop. The internal order of shutdown is roughly:

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

The first stop is recorded in `_stopped`, a future that resolves to a WorkerStopped (a CleanStop for expiry, a stop() that
interrupts nothing, or the parent task returning), logs it, and calls on_stopped. Later stops don't change that, and every
call from then on is refused with it. Whatever reads it gets a copy, keeping its cause, so callers' tracebacks don't pile
up on one shared exception.

Once the parent task and every call have finished, the loop cancels any tasks left on it (work that the parent task or
calls spawned and didn't wait for), gives them 0.05s, shuts down async generators and the default executor (abandoning, with a RuntimeWarning,
threads that haven't finished by then), and closes. Only then do stop()'s waiters wake, and on_shutdown run.

Invariants
----------
- Every call resolves. One the loop drops unrun as it closes fails with why the worker stopped, which stop()'s waiters
  get as its result: nothing waits forever on a worker that has finished.
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
  that take asyncio futures, use asyncio.wrap_future(call). It can be awaited in a trio task too (anyio's, on trio,
  included), whose cancellation cancels the job, as on asyncio.
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
a drain lets finish), from any thread, the worker's own included. It returns an AwaitableFuture that resolves to why
the worker stopped (a WorkerStopped, which it returns rather than raises): once the worker has finished, or with
wait=False, once the stop has begun. On the worker's own thread,
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
  _fork_safety (WorkerForked is a WorkerBroken, itself a WorkerStopped). That goes for a call that forked too, whose
  child runs on in a copy of the worker's loop (one asyncio no longer counts as running): the worker carries on only in
  the parent.
- If you perform blocking waits on this worker when completed or interrupted code inside the worker is garbage collected
  or during handling of its exception classes, you may break this class's invariant (this is generally also true for all
  other asyncio code in Python).
- How interrupted calls reach callers that await them relies on asyncio.wrap_future() converting exactly
  concurrent.futures.CancelledError to asyncio's, and nothing else: a private detail of asyncio
  (asyncio.futures._convert_future_exc).
"""
import asyncio
import logging
from contextlib import suppress
from contextvars import Context, copy_context
from functools import cached_property, wraps
from threading import Thread, current_thread, main_thread, Condition
from typing import Any, Callable, Coroutine, final
from weakref import WeakSet

import sniffio
from attrs import Factory, define, field, validators

from async_anywhere._util import validate_coroutine_function, LockProtected, copy_exception
from async_anywhere.captive import _fork_safety
from async_anywhere.captive._exceptions import WorkerInterrupted, WorkerStopped, WorkerForked
from async_anywhere.captive._stopped import WorkerStopFuture
from async_anywhere.captive._timer import Timer
from async_anywhere.futures import AwaitableFuture


async def _until_cancelled(_):
    await asyncio.Future()

def _timer_factory(worker: CaptiveWorker):
    return Timer(
        loop=worker._loop,
        callback=lambda: worker._stopped.clean_stop("expired", worker._calls) and worker._interrupt(True),
    )

@final
@define(
    kw_only=True,
    eq=False, # Make equality and hashing use object identity alone.
    frozen=True, # Immutable fields
    repr=False, # Repr gets extremely verbose and not useful.
    auto_attribs=False,
)
class CaptiveWorker:
    # Run with the worker, on its loop, as soon as that's running: until a stop cancels it, by default. See Usage.
    parent_task: Callable[[CaptiveWorker], Coroutine[Any, Any, Any]] = field(
        default=_until_cancelled, validator=validate_coroutine_function
    )
    # Each called once, with the worker, on its thread: as it records why it has stopped, sometimes outside its loop,
    # and once it has finished (its loop has closed), just after stop()'s waiters have been woken. They MUST NOT BLOCK,
    # nor do anything but brief, constant-time work. What they raise is logged (by concurrent.futures), not raised.
    on_stopped: Callable[[CaptiveWorker], Any] = field(default=id, validator=validators.is_callable())
    on_shutdown: Callable[[CaptiveWorker], Any] = field(default=id, validator=validators.is_callable())
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
    _stopped: WorkerStopFuture = field(init=False, default=Factory(WorkerStopFuture, takes_self=True))
    _finished: WorkerStopFuture = field(init=False, default=Factory(WorkerStopFuture, takes_self=True))  # For stop()
    # The parent task's task, once it has started, which the constructor waits for: see _main()
    _runner: AwaitableFuture[asyncio.Task] = field(init=False, factory=AwaitableFuture)
    _expiry: Timer = field(init=False, default=Factory(_timer_factory, takes_self=True))
    # Calls' futures, which _finalize() fails if the worker won't finish them (if the loop drops them unrun as it closes,
    # say). Weak, as a call that's done, and dropped by its caller, needs nothing more.
    _submitting: LockProtected[WeakSet[AwaitableFuture]] = field(init=False, factory=lambda: LockProtected(WeakSet()))
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
        # Whatever fails _finished fails _stopped too, if it's pending: in case anything ever fails just the one
        self._finished.add_done_callback(lambda done: (why := done.exception_or_none()) and self._fail(self._stopped, why))
        self._stopped.add_done_callback(lambda _: self.in_worker() and self.on_stopped(self))
        self._finished.add_done_callback(lambda _: self.in_worker() and self.on_shutdown(self))
        _fork_safety.workers.add(self)  # Before the thread starts, so the fork handler knows of every worker running
        self._thread.start()
        self._runner.result()  # So that, once constructed, the worker is running

    def bridge[**P, T](self, func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, AwaitableFuture[T]]:
        self._finished.raise_if_failed()  # Best-effort: it's calling bridged functions that's always checked

        @wraps(func)
        def call(*args: P.args, **kwargs: P.kwargs) -> AwaitableFuture[T]:
            # A call's future, with its job queued for _start(). Refused (raising why it stopped) once the worker has stopped:
            # _job() would refuse it too, but without waiting for the loop is better. And so _finalize(), failing what's
            # pending once the worker has stopped, after taking _submitting, can't miss anything.
            in_loop = self.in_worker()
            loop = self._loop
            future = AwaitableFuture(prevent_blocking_wait_in_loops=loop)
            context = copy_context()
            if context.get(sniffio.current_async_library_cvar):
                # Which anyio.run() sets to its backend: left as it is, a caller's on trio would have anyio, in the job,
                # take it to be on trio too. As trio's threads do, cleared, so that sniffio looks for the job's loop.
                context.run(sniffio.current_async_library_cvar.set, None)
            with self._submitting as pending:
                if self._stopped.done():
                    self._finished.raise_if_failed()  # Only here, as _stopped is always done once _finished has failed
                    raise self._stopped.result(timeout=0)
                pending.add(future)
                # Only queues a callback, so never waits for the worker's loop. It runs in a copy of the caller's context.
                if in_loop:
                    loop.call_soon(self._start, future, func, args, kwargs, context=context)
                else:
                    loop.call_soon_threadsafe(self._start, future, func, args, kwargs, context=context)
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

        self._finished.raise_if_failed()
        # Eagerly, rather than leaving it to _finished's own check: that catches result(), but not a call awaiting it,
        # which would deadlock, as a drain would wait for that very call.
        if wait and self.in_worker():
            raise RuntimeError("stop(wait=True) on the worker's own thread would wait for itself")

        def begin():  # On the worker's loop. Not a call, so it isn't refused once the worker has stopped: it can cut a drain short.
            if interrupt and self._calls:
                self._stopped.unclean_stop("manually stopped", self._calls)
            else:
                self._stopped.clean_stop("stopped", self._calls)
            if interrupt or not self._calls:
                self._interrupt(interrupt)  # Otherwise the last call to end does: see _job()

        with suppress(RuntimeError):  # The loop has closed, so the worker has already stopped
            self._loop.call_soon_threadsafe(begin)  # Queued even on the worker's thread, behind calls made before it
        return self._finished if wait else self._stopped

    def _start(self, future: AwaitableFuture, func, args, kwargs):
        # If the job was cancelled by its caller or failed by the worker e.g. forking, do nothing.
        if future.done():
            return
        job = self._job(func, args, kwargs, future)
        # Adjust the job's name to reflect the function being called, for more legible tracebacks:
        job.__qualname__ += f"[{func.__qualname__}]"
        # eager_start=True ensures that the job is queued to run before anything dispatched after it. Not setting this
        # leads to confusing priority semantics between jobs started inside the worker vs. outside.
        task = self._loop.create_task(job, eager_start=True)

        def forward(fut):
            if fut.cancelled():

                def cancel():
                    # Not once the worker has interrupted it: an interrupt also cancelling what awaits it (the parent task's
                    # TaskGroup cancelling its children, say) mustn't cancel it twice.
                    if not (self._stopped.interrupted and task.cancelling()):
                        task.cancel()

                with suppress(RuntimeError):  # The loop has closed
                    self._loop.call_soon_threadsafe(cancel)

        future.add_done_callback(forward)

    def _fail(self, future: AwaitableFuture, error: BaseException | None = None) -> bool:
        return future.set_exception_if_not_done(error or self._finished.exception_or_none() or self._stopped.result(timeout=0))

    def _log(self, level: int, msg: str, *args, **kwargs):
        # Only on the worker's thread, as for hooks: in a forked process, a handler's stream may be locked forever
        if self.in_worker():
            self.logger.log(level, msg, *args, stacklevel=2, **kwargs)

    def _after_fork_in_child(self, pid: int):
        """
        In the child process of a fork, CaptiveWorker instances are invalidated. This is true even if code running on a
        CaptiveWorker's thread did the fork; asyncio's at-fork semantics are cloudy enough that it's not worth trying to
        preserve running workers' states after fork--and that's without getting into what happens when the parent and
        child both try to interact with a worker!

        When fork occurs, this method ensures the following is true in the child process for every CaptiveWorker that
        existed when fork(2) occurred:
        - worker._submitting is unlocked (in case a @bridge call was being dispatched at the moment of fork(2)).
        - The worker's AwaitableFutures get fresh conditions (in case another thread was using one).
        - The worker's stop bit is set with a reason indicating that fork(2) occurred, unless it had stopped already.
        - The worker is marked as shut down.
        """
        self._submitting._lock.acquire(timeout=0)
        self._submitting._lock.release()

        with self._submitting as pending:
            for future in (self._stopped, self._finished, self._runner, *pending):
                future._condition = Condition()

        self._stopped.unclean_stop(f"forked into process {pid}", self._calls)  # Unless it had stopped
        if self._fail(self._finished, WorkerForked()):
            self._finalize()

    def _finalize(self):
        # Wait for any jobs that were being submitted at the moment of shutdown to finish being submitted. Because
        # _stopped has been set already everywhere this method is called, serializing here ensures that any submits
        # after this point in time will observe stopped.done and fail.
        with self._submitting as pending:
            to_abort = list(pending)
            pending.clear()
        while len(to_abort):
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
                stopping = self._stopped.unclean_stop(f"parent task raised {ex!r}", self._calls, ex)
            else:
                stopping = self._stopped.clean_stop("parent task returned", self._calls)
            if stopping:
                # The parent task ending was the stop, so it interrupts calls, as stop(interrupt=True) would
                self._interrupt(True)
            # Let calls still in flight finish (or, if they were interrupted, clean up) before re-cancelling them and
            # aborting them.
            if self._calls:
                loop.run_until_complete(asyncio.wait(self._calls.copy()))
        except BaseException as exc:
            # Should never happen, except for a KeyboardInterrupt or SystemExit from the parent task or a call: asyncio
            # lets those escape the loop from any task. Record the crash (logging it, if it's the first stop), so that
            # calls refused from now on say why.
            self._stopped.unclean_stop("the worker's event loop crashed", self._calls, exc)
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
                            self._stopped.interrupted = True
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
                if in_worker:
                    self._loop.close()  # Shuts the executor down without waiting, if it hasn't been already
                # The very last action: stop()'s waiters wait for it, and on_shutdown() follows
                self._finished.set_result_if_not_done(self._stopped.result())

    def _interrupt(self, again: bool):
        # Once the worker has stopped: cancels the parent task and every call in flight. Each time with `again` (for
        # stop(interrupt=True), expiry, and the parent task ending of its own accord); otherwise (for a stop that
        # doesn't interrupt, once no calls are left in flight) only if the worker hasn't interrupted anything yet.
        if again or not self._stopped.interrupted:
            for task in {self._runner.result(timeout=0), *self._calls}:
                if task.cancel():
                    self._stopped.interrupted = True

    async def _job(self, func, args, kwargs, future: AwaitableFuture):
        """
        Runs a call on the worker's event loop. All calls are submitted by _start().
        Only raises in the thread on a crash (e.g. a cancel race or a broken event loop); all errors from user code are
        forwarded back to the user if the user is still accepting responses (that is, the waiting future hasn't been
        cancelled or aborted with an interrupt error).
        """

        # We're a little precious about performance in this method since it's on the hot path:
        task, stopped, calls, expiry = asyncio.current_task(), self._stopped, self._calls, self._expiry
        try:
            if stopped.done():
                raise stopped.result()  # Raise if the worker is stopping or broken/shut down
            calls.add(task)
            expiry.reset(None) # Suspend the idle timeout
            try:
                result = await func(*args, **kwargs)
            finally:
                calls.remove(task)
                # A drain (a stop that leaves calls in flight to finish) cancels the parent task only once the last has
                # ended: here. Nothing else would, unless the parent task ended of its own accord, or a later stop()
                # interrupted.
                if len(calls) == 0:
                    # Not a done-callback on _stopped: that fires once, as the stop is recorded (begin() covers there
                    # being no calls then), on whichever thread records it, rather than as the last call ends.
                    if stopped.done():
                        self._interrupt(False)
                    expiry.reset(self.idle_timeout)

        except asyncio.CancelledError as ex:
            # Check if we got a CancelledError because the worker was interrupted with stop(interrupt=True). If so, this
            # job's waiters get an error indicating where the job was cancelled, with exception-chain-context about why
            # the worker stopped.
            if stopped.interrupted and task.cancelling():
                # TODO is timeout=0 safe here? It should be...
                error = WorkerInterrupted.from_cancellation(ex, stopped.result(timeout=0))
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
            if not self._fail(future, ex) or not isinstance(ex, (Exception, asyncio.CancelledError)):
                raise
        else:
            # Transmit a successful result to the user.
            # NB: if the caller cancelled it before it finished here, we WANT users to see the "task exception never
            # retrieved" error for InvalidStateError raised from the below call.
            future.set_result(result)
