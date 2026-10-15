"""A captive worker: a thread with an asyncio event loop of its own, hosting one client, whose operations any thread or
event loop can call. It's a connection pool of size one, for upstreams that allow a single connection at a time, and for
clients that must stay on the loop that created them. It uses the standard library alone.

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
loop other than with asyncio.new_event_loop(), as for asyncio.Runner.

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

From any thread, it returns an AwaitableFuture: a concurrent.futures.Future, which can also be awaited on any loop.
- It can be waited on repeatedly, and from several threads at once. One nobody waits on runs all the same, but what it
  raises goes unreported (where asyncio would log a task's as never retrieved).
- Giving up a result() or exception() (as it times out, say) leaves the call running, as for any concurrent future.
  Cancelling the future, or a task awaiting it, cancels the job, without waiting for it to finish cancelling.
- Waiting on an unfinished call with result() or exception() on the worker's own loop raises RuntimeError, as nothing
  could finish it.
- But it isn't a coroutine, nor an asyncio future: create_task(), TaskGroup.create_task() and asyncio.wait() reject it, and
  inspect.iscoroutinefunction() is False for bridged functions (so frameworks may take them for synchronous). For APIs
  that take asyncio futures, use asyncio.wrap_future(call). Awaiting one needs an asyncio loop: trio callers can wait
  with trio.to_thread.run_sync(call.result).
- The job runs in a task of its own on the worker's loop, with a copy of the caller's context variables as they were when
  the call was made. Changes it makes to them don't reach the caller. Library state the copy carries, tied to
  the caller's thread, may or may not keep working on the worker's: for a synchronous caller (through asgiref's
  async_to_sync()), the job's thread-sensitive sync_to_async() calls run on the caller's thread with asgiref 3.11, but on
  another with 3.12.
- Its return value and exceptions reach the caller as the same objects, raised again there. So they, and the arguments,
  cross between loops and threads: they mustn't be bound to either loop (asyncio futures, tasks, streams, async
  generators, clients), and asyncio's Event, Queue and Lock aren't thread-safe.
- Exceptions differ in four cases:
  - Refused, as the worker had already stopped: WorkerStopped.
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
- A worker may stop just as a call is made, by expiry for one: callers that should carry on can retry on CleanStop, with
  a fresh worker.
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
import concurrent.futures
import logging
from contextlib import suppress
from contextvars import Context
from functools import cached_property, wraps
from threading import Thread, current_thread, main_thread
from typing import Any, Callable, Coroutine, final
from weakref import WeakSet

from attrs import Factory, define, field, validators

from async_anywhere._util import validate_coroutine_function
from async_anywhere.captive import _fork_safety
from async_anywhere.captive._exceptions import WorkerInterrupted, WorkerStopped, copied
from async_anywhere.captive._stopped import WorkerStopFuture
from async_anywhere.captive._timer import _Timer
from async_anywhere.futures import AwaitableFuture


async def _until_cancelled(_):
    await asyncio.Future()  # The default parent task



def _timer_factory(worker: CaptiveWorker):
    return _Timer(
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
    _runner: concurrent.futures.Future[asyncio.Task] = field(init=False, factory=concurrent.futures.Future)
    # Calls' futures, which _finalize() fails if the worker won't finish them (if the loop drops them unrun as it closes,
    # say). Weak, as a call that's done, and dropped by its caller, needs nothing more.
    _pending: WeakSet[AwaitableFuture] = field(init=False, factory=WeakSet)
    _expiry: _Timer = field(init=False, default=Factory(_timer_factory, takes_self=True))
    _thread: Thread = field(
        init=False,
        default=Factory(lambda self: Thread(target=self._main, name=self.logger.name, daemon=True), takes_self=True),
    )

    @cached_property
    def _loop(self) -> asyncio.AbstractEventLoop:
        # Made by the fields that need it, as the worker's constructed, so on one thread. Run by _main() on the worker's
        # thread: until then, work is just queued on it.
        loop = self.loop_factory()
        if not isinstance(loop, asyncio.AbstractEventLoop) or loop.is_running() or loop.is_closed():
            raise TypeError(f"loop_factory must make a new asyncio event loop, not {loop!r}")
        return loop

    def __attrs_post_init__(self):
        # Whatever fails _finished fails _stopped too, if it's pending: in case anything ever fails just the one
        self._finished.add_done_callback(lambda done: (why := done.failure()) and self._fail(self._stopped, why))
        self._stopped.add_done_callback(lambda _: self.in_worker and self.on_stopped(self))
        self._finished.add_done_callback(lambda _: self.in_worker and self.on_shutdown(self))
        _fork_safety.workers.add(self)  # Before the thread starts, so the fork handler knows of every worker running
        self._thread.start()
        self._runner.result()  # So that, once constructed, the worker is running

    def bridge[**P, T](self, func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, AwaitableFuture[T]]:
        """`func`, bridged to the worker (see the module's docstring).

        Note: do not assume that the returned future is unique/newly constructed, depending on worker state it may not be.
        """
        if broken := self._finished.failure():  # Best-effort: it's calling bridged functions that's always checked
            raise broken

        @wraps(func)
        def call(*args: P.args, **kwargs: P.kwargs) -> AwaitableFuture[T]:
            # A call's future, with its job queued for _start(). Refused (failed with why it stopped) once the worker has stopped:
            # _job() would refuse it too, but without waiting for the loop is better. And so _finalize(), failing what's
            # pending once the worker has stopped, after taking _stopped's lock, can't miss anything.
            if broken := self._finished.failure():
                raise broken
            in_loop = self.in_worker
            with self._stopped:  # As a lock, so _main() can't miss a submission as it fails what's pending
                if self._stopped.done():
                    self._fail(refused := AwaitableFuture(prevent_blocking_wait_in_loops=(self._loop,)))
                    return refused
                future = AwaitableFuture(prevent_blocking_wait_in_loops=(self._loop,))
                self._pending.add(future)
                # Only queues a callback, so never waits for the worker's loop. It runs in a copy of the caller's context.
                queue = self._loop.call_soon if in_loop else self._loop.call_soon_threadsafe
                queue(self._start, future, func, args, kwargs)
            return future

        return call

    @property
    def in_worker(self) -> bool:
        # On the worker's thread, so in its loop, as no other may run there. And in its own process: not in one forked
        # from a call, where that thread is the main one.
        return current_thread() is self._thread and self._thread is not main_thread()

    def stop(self, interrupt=False, wait=True) -> AwaitableFuture[WorkerStopped]:
        """Stop the worker, and return a future that resolves to why once the worker has finished, or with `wait=False`, once
        the stop has begun.

        Calls are refused from then on. Those in flight are cancelled, along with the parent task, each time this is
        called with `interrupt`, or left to finish before the parent task is (a drain, which a later interrupting stop()
        can cut short). On the
        worker's own thread, `wait` is an error, as it would wait for itself, and interrupting cancels the calling task
        too."""

        def begin():  # On the worker's loop. Not a call, so it isn't refused once the worker has stopped: it can cut a drain short.
            if interrupt and self._calls:
                self._stopped.unclean_stop("manually stopped", self._calls)
            else:
                self._stopped.clean_stop("stopped", self._calls)
            if interrupt or not self._calls:
                self._interrupt(interrupt)  # Otherwise the last call to end does: see _job()
        if broken := self._finished.failure():
            raise broken
        if wait and self.in_worker:
            raise RuntimeError("stop(wait=True) on the worker's own thread would wait for itself")
        with suppress(RuntimeError):  # The loop has closed, so the worker has already stopped
            self._loop.call_soon_threadsafe(begin)  # Queued even on the worker's thread, behind calls made before it
        return self._finished if wait else self._stopped

    def _start(self, future: AwaitableFuture, func, args, kwargs):
        # Runs a call in a task of its own, which cancelling the future cancels. Not if it's done already: cancelled by its
        # caller, or failed, in a process forked from a call, whose copy of the loop runs on.
        if future.done():
            return
        job = self._job(func, args, kwargs, future)
        job.__qualname__ += f"[{func.__qualname__}]"  # Only shown, in reprs, so needn't make an identifier
        task = self._loop.create_task(job, eager_start=True)  # So it's in flight before anything queued after it

        def cancel():
            # Not once the worker has interrupted it: an interrupt also cancelling what awaits it (the parent task's
            # TaskGroup cancelling its children, say) mustn't cancel it twice.
            if not (self._stopped.interrupted and task.cancelling()):
                task.cancel()

        def forward(_):
            if future.cancelled():
                with suppress(RuntimeError):  # The loop has closed
                    self._loop.call_soon_threadsafe(cancel)

        future.add_done_callback(forward)

    def _fail(self, future: concurrent.futures.Future, error: BaseException | None = None) -> bool:
        # Whether it took `error` (by default, why the worker stopped, or why it can't be used at all): not if it finished
        # after all, or its caller cancelled it, or it has been failed already
        try:
            future.set_exception(error or self._finished.failure() or self._stopped.result())
            return True
        except concurrent.futures.InvalidStateError:
            return False

    def _finalize(self):
        # Wait for any jobs that were being submitted at the moment of shutdown to finish being submitted. Because
        # _stopped has been set already everywhere this method is called, serializing here ensures that any submits
        # after this point in time will observe stopped.done and fail.
        with self._stopped:
            pending = list(self._pending)
        while len(pending):
            self._fail(pending.pop())  # Never run, or abandoned (or here, in a forked process)

    def _main(self):
        async def run():
            self._expiry.reset(self.idle_timeout)  # "Start the clock!" - Idris Elba
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
            # Let calls still in flight finish (or, interrupted, clean up): the teardown would cancel them.
            if self._calls:
                # Only wait if we have any; asyncio.wait raises on empty
                loop.run_until_complete(asyncio.wait(list(self._calls)))
        except BaseException as exc:
            # Should never happen, except for a KeyboardInterrupt or SystemExit from the parent task or a call: asyncio
            # lets those escape the loop from any task. Record the crash (logging it, if it's the first stop), so that
            # calls refused from now on say why.
            self._stopped.unclean_stop("the worker's event loop crashed", self._calls, exc)
            raise
        finally:
            self._fail(self._runner)  # So the constructor raises why, rather than waiting forever, if run() never ran
            try:
                # TODO log the non-call/non-run tasks this cancels
                tasks = asyncio.all_tasks(loop)
                for task in tasks:
                    if task.cancel():
                        # Calls among them (after a crash) tell their waiters why: see _job()
                        self._stopped.interrupted = True
                # TODO: log the threads this abandons, per worker: asyncio only warns, with a RuntimeWarning shown once.
                # Created before the wait below starts its clock, so its own timeout fires (and warns) first.
                tasks.add(loop.create_task(loop.shutdown_default_executor(timeout=0.05)))
                tasks.add(loop.create_task(loop.shutdown_asyncgens()))
                loop.run_until_complete(asyncio.wait(tasks, timeout=0.05))
            finally:
                self._finalize()
                self._calls.clear()
                self._pending.clear()
                self._loop.close()  # Shuts the executor down without waiting, if it hasn't been already
                # The very last action: stop()'s waiters wait for it, and on_shutdown() follows
                with suppress(concurrent.futures.InvalidStateError):
                    self._finished.set_result(self._stopped.result())

    def _interrupt(self, again: bool):
        # Once the worker has stopped: cancels the parent task and every call in flight. Each time with `again` (for
        # stop(interrupt=True), expiry, and the parent task ending of its own accord); otherwise (for a stop that
        # doesn't interrupt, once no calls are left in flight) only if the worker hasn't interrupted anything yet.
        if again or not self._stopped.interrupted:
            for task in {self._runner.result(timeout=0), *self._calls}:
                if task.cancel():
                    self._stopped.interrupted = True

    async def _job(self, func, args, kwargs, future: AwaitableFuture):
        # Runs a call (see _start()), and settles its future. It raises only crashes, and what the future can't take
        # (cancelled by its caller, say), which asyncio then logs, if it's an error, as for any task.
        task = asyncio.current_task(self._loop)
        try:
            if self._stopped.done():
                raise self._stopped.result()  # Which raises itself, if the worker is broken
            self._calls.add(task)
            self._expiry.reset(None) # Suspend the idle timeout
            try:
                result = await func(*args, **kwargs)
            finally:
                self._calls.remove(task)
                # A drain (a stop that leaves calls in flight to finish) cancels the parent task only once the last has
                # ended: here. Nothing else would, unless the parent task ended of its own accord, or a later stop()
                # interrupted.
                if self._stopped.done() and not self._calls:
                    self._interrupt(False)
                self._expiry.reset(None if len(self._calls) > 0 else self.idle_timeout) # Restart the idle timeout
        except BaseException as ex:
            error = ex  # Raised by func itself, if a CancelledError, as its task isn't being cancelled
            if isinstance(ex, asyncio.CancelledError) and task.cancelling() and self._stopped.interrupted:
                # Its waiters get where the job was cancelled, raised from (a copy of) why the worker stopped, itself raised
                # from what caused that, if anything. (Otherwise, its caller cancelled the future.)
                error = WorkerInterrupted.from_cancellation(ex, self._stopped.result())
            elif type(ex) in (KeyboardInterrupt, SystemExit):
                error = copied(ex)  # This thread raises the original too, crashing the loop
            if not self._fail(future, error) or not isinstance(ex, Exception | asyncio.CancelledError):
                raise
        else:
            with suppress(concurrent.futures.InvalidStateError):  # Its caller cancelled it
                future.set_result(result)
