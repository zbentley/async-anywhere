"""A captive worker: a thread with an asyncio event loop of its own, hosting one client, whose operations any thread or
event loop can call. It's a connection pool of size one, for upstreams that allow a single connection at a time, and for
clients that must stay on the loop that created them. It uses the standard library alone.

Usage
-----
Subclass CaptiveWorker. Connect, and run the client, in _run(). Expose its operations with `worker.bridge(func)` (or as
@worker.bridge), which bridges an async function, called with the worker as its first argument: so a method of the
worker's own, bridged in its __init__() as `self.fetch = self.bridge(self._fetch)`, gets it twice, as (self, worker, ...).
Set idle_timeout to stop a worker that has gone that many seconds without calls. Override on_stop_requested() and
on_shutdown() to act as the worker stops, and once it has finished: each is called once, on the worker's thread, and must
never block. Subclass CachedCaptiveWorker instead, and get workers with `await Subclass.get(*args, **kwargs)`, to keep one
per class and arguments, replaced once it has stopped.

Lifecycle
---------
Constructing a worker starts its thread, which runs _run() before anything else. _run() can restart the idle countdown, on
activity of its own, with _reschedule().

## Worker stop:

Once a worker is stopped, it will not accept new jobs. In-progress jobs and `_run` may be interrupted, or may continue running, depending on how a stop was triggered, see "Interruption" below.

After a worker has been stopped, new attempts to call any bridge methods will fail with an error.

Workers are stopped in these situations:

- A call to `stop()` is made.
- `_run()` finishes.
- After a fork, workers inherited by a child process register as being stopped in that process.

## Worker shutdown:

A worker is shut down once it has terminated its event loop. The internal order of shutdown is roughly:

1. Ensure the worker is stopped, or stop it.
2. Ensure `_run` and all bridge jobs in flight have completed (see "Interruption" for cancellation semantics).
3. Abort all other active work on the captive event loop.
4. Stop the captive event loop and remove references to all active work.

After a worker has shut down, attempts to retrieve the result of any incomplete bridge calls initiated before the shutdown will fail with an error.

Shutdown occurs in the following situations:


- If `_run` or any bridge job raises  `SystemExit` or `KeyboardInterrupt`, a worker will be shut down.
- After `_run` finishes (via ordinary completion, expiration, or interruption),the worker is shut down.
- After a fork, workers inherited by a child process register as being shut down in that process.

## Interruption

User code is interrupted (sent a cancellation) in these situations:

`_run`:
    - The idle timeout expires.
    - `stop(interrupt=True)` is called.
    - `stop(interrupt=False)` is called, once no in-progress bridge jobs are running. This interruption will only be sent to `_run` once, and will only be sent if `_run` has not been interrupted for any other reason.

Bridge jobs:
    - `stop(interrupt=True)` is called.
    - `_run` completes unexpectedly (returns or raises an exception without `stop()` having been called or expiration having
      occurred). This has the same effect as calling `stop(interrupt=True)`.
    - A bridge job's caller cancels it (e.g. via `worker.bridge_call().cancel()` or by triggering cancellation during
      `await worker.bridge_call(...)`).

Other tasks created inside the worker (e.g. via worker code using `asyncio.shield` or un-awaited `asyncio.Task`s):
    - User code triggers an ordinary cancellation.
    - The worker finishes shutting down.

**Note:** a cancellation is sent to `_run` and running bridge jobs *each* time `stop(interrupt=True)` is called. This can
lead to them receiving multiple cancellation events from the worker. Code which swallows or deduplicates cancellations (e.g. `TaskGroup`) may behave unexpectedly when this occurs.

**Note:** code that chooses to ignore cancellations may miss worker interrupts or expirations.

The first stop is recorded in `_stopped`, a future that fails with a WorkerStopped (a CleanStop for expiry, a stop() that
interrupts nothing, or _run() returning), logs it, and calls on_stop_requested(). Later stops don't change that, and every
call from then on is refused with it. Whatever reads it gets a copy, keeping its cause, so callers' tracebacks don't pile
up on one shared exception.

Once _run() and every call have finished, the loop cancels any tasks left on it (work that _run() or calls spawned and
didn't wait for), gives them 0.05s, shuts down async generators and the default executor (abandoning, with a RuntimeWarning,
threads that haven't finished by then), and closes. Only then do stop()'s waiters wake, and on_shutdown() run: a
CachedCaptiveWorker's leaves get()'s cache. get() replaces a worker only once it has finished, so never has two workers,
and two connections, for the same arguments.

Invariants
----------
- Every call resolves. One the loop drops unrun as it closes fails with why the worker stopped, as do stop()'s waiters:
  nothing waits forever on a worker that has finished.
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
Calling a bridged function is like create_task() with an eager task factory: the work may start right away, and runs in
the background, whatever the caller's await points (calls made together run concurrently).

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

stop(interrupt=False, wait=True) isn't bridged. It begins the stop as it's called, and returns an AwaitableFuture that
raises why the worker stopped: once the worker has finished, or with wait=False, once the stop has begun. On the worker's
own thread, wait=True is an error, as it would wait for itself, and interrupt=True cancels the calling task as it does any
other.

Caveats
-------
- A task that ignores cancellation holds stopping up forever if it's _run() or a call: see Interruption.
- Tasks that _run() and calls spawn are abandoned as the loop closes (asyncio logs "Task was destroyed but it is
  pending!"), so _run() should wait for whatever it spawns.
- Synchronous code blocking the worker's loop delays everything on it: calls start, and cancellations land, once it yields.
- KeyboardInterrupt or SystemExit from _run() or a call crash the loop, as asyncio lets them escape any task. The crash is
  recorded as the stop (and logged, if it's the first), and re-raised on the worker's thread, and by the call that raised
  it, in its caller. Other calls in flight are interrupted, with only the teardown's 0.05s to clean up.
- CaptiveWorker.__init__ starts the thread, so _run() may begin before a subclass's __init__ has finished: set what it needs
  before calling super().__init__(). Make the client in _run(), not __init__(), which runs on the constructing thread,
  perhaps in a caller's loop, which a client made there would be bound to.
- Once the worker has stopped, bridged functions are refused even to _run(): its cleanup must use the client directly. And
  a drain waits for calls that _run()'s own tasks have in flight, as for any other.
- anyio's task groups cut their children's cleanup short as they're cancelled, unless it's shielded: as ever with anyio.
- A worker that CachedCaptiveWorker.get() returns may stop before it's called, by expiry for one: callers that should
  carry on can retry on CleanStop, with a fresh get(). get() keys workers by its arguments as given, so get("x") and get(key="x") make two.
- get() can't replace a stopped worker from that worker's own thread, as waiting for it to finish would wait for itself.
- Workers run on daemon threads: one not shut down before the interpreter exits stops without cleaning up (_run()'s
  cleanup never runs). Shut workers down with atexit if that matters.
- A process forked with workers alive inherits copies of them, but not their threads: in the child, each is stopped (a
  WorkerStopped, logged there), with its calls in flight failed, and calls refused. That goes for a call that forked too,
  whose child runs on in a copy of the worker's loop (one asyncio no longer counts as running): the worker carries on only
  in the parent. This relies on Python's handling of the fork, which native code calling fork() bypasses unless it calls
  PyOS_AfterFork_Child(), as CPython requires anyway, for the child to run Python. get() hangs in the child,
  though, if its cache has run anything in the parent: it constructs workers through asgiref's sync_to_async(), whose
  executor's thread doesn't survive the fork either.
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
import os
from contextlib import suppress
from contextvars import Context
from functools import wraps
from threading import Lock, Thread, current_thread, main_thread
from typing import Any, ClassVar, Concatenate, Literal, Never, NoReturn, Self, Callable, Coroutine
from weakref import WeakSet

from async_anywhere.captive._exceptions import WorkerInterrupted
from async_anywhere.captive._stopped import WorkerStopFuture
from async_anywhere.futures import AwaitableFuture




class CaptiveWorker:
    idle_timeout: ClassVar[float] = float('inf')
    _WORKERS: ClassVar[WeakSet[CaptiveWorker]] = WeakSet()  # For _after_fork()

    def __init__(self):
        self.logger = logging.getLogger(f"captive[{type(self).__name__}]")
        # Bridged calls in flight: their jobs' tasks. They hold off idle expiry, and drains wait for them; see _interrupt().
        self._calls: set[asyncio.Task] = set()
        # The loop is created here but run by _main() on the worker's thread: until then, work is just queued on it.
        self._loop = asyncio.new_event_loop()
        self._stopped = WorkerStopFuture(self._loop, self.logger)
        self._finished = WorkerStopFuture(self._loop, self.logger)  # Which stop() waits for: see _main()
        self._stopped.add_done_callback(lambda _: self._on_own_thread and self.on_stop_requested())
        self._finished.add_done_callback(lambda _: self._on_own_thread and self.on_shutdown())

        # Created now, so the loop runs _run() before any call, and in a fresh context, which keeps the constructing
        # thread's context variables out of it.
        self._runner = self._loop.create_task(self._run(), context=Context())
        # Calls' futures, which _finalize() fails if the worker won't finish them (if the loop drops them unrun as it closes,
        # say). Weak, as a call that's done, and dropped by its caller, needs nothing more.
        self._pending: WeakSet[AwaitableFuture] = WeakSet()
        self._submitting = Lock()  # So _main() can't miss a submission as it fails what's pending: see bridge()
        self._interrupted = False  # Whether the worker has cancelled user code: _run(), calls, or what they left running
        self._expiry: asyncio.TimerHandle | None = None  # The idle countdown: see _reschedule()
        self._reschedule()  # "Start the clock!" - Idris Elba
        self._thread = Thread(target=self._main, name=self.logger.name, daemon=True)
        self._thread.start()
        self._WORKERS.add(self)

    def bridge[**P, T](
        self, func: Callable[Concatenate[Self, P], Coroutine[Any, Any, T]]
    ) -> Callable[P, AwaitableFuture[T]]:
        """`func`, bridged to the worker (see the module's docstring), which it's called with as its first argument."""

        @wraps(func)
        def call(*args: P.args, **kwargs: P.kwargs) -> AwaitableFuture[T]:
            # A call's future, with its job queued for _start(). Refused (the stop future) once the worker has stopped:
            # _job() would refuse it too, but without waiting for the loop is better. And so _finalize(), failing what's
            # pending once the worker has stopped, after taking _submitting, can't miss anything.
            in_loop = self.in_worker
            with self._submitting:
                if self._stopped.done() or self._dead_or_forked():
                    return self._stopped
                future = AwaitableFuture(self._loop)
                self._pending.add(future)
                # Only queues a callback, so never waits for the worker's loop. It runs in a copy of the caller's context.
                queue = self._loop.call_soon if in_loop else self._loop.call_soon_threadsafe
                queue(self._start, future, func, args, kwargs)
            return future

        return call

    def on_stop_requested(self):
        """For subclasses: called once, on the worker's thread, as the worker records why it has stopped. It MUST NOT
        BLOCK, nor do anything but brief, constant-time work: it's a plain method, run synchronously in the middle of
        stopping, sometimes outside the worker's loop. What it raises is logged (by concurrent.futures), not raised."""

    def on_shutdown(self):
        """For subclasses: called once, on the worker's thread, once the worker has finished, just after stop()'s waiters
        have been woken (its loop has closed). It MUST NOT BLOCK, nor do anything but brief, constant-time work: it's a
        plain method, run synchronously. What it raises is logged (by concurrent.futures), not raised."""

    @property
    def in_worker(self) -> bool:
        return current_thread() is self._thread  # So in the worker's loop, as no other may run on its thread

    @property
    def _on_own_thread(self) -> bool:
        # On the worker's thread, in its own process: not in one forked from a call, where that thread is the main one
        return current_thread() is self._thread and self._thread is not main_thread()

    async def _run(self):
        await asyncio.Event().wait()  # Until a stop cancels it

    def _reschedule(self):
        # Restarts the idle countdown, which runs only while no calls are in flight. _run() may call it, on activity of its
        # own. Once the worker has stopped, the countdown may still run, but its firing does nothing, the stop having been
        # recorded already.
        if self._expiry:
            self._expiry.cancel()
        if not self._calls:
            self._expiry = self._loop.call_later(
                self.idle_timeout,
                lambda: self._stopped.clean_stop("expired", self._calls) and self._interrupt(True),
            )


    def stop(self, interrupt=False, wait=True) -> AwaitableFuture[NoReturn]:
        """Stop the worker, and return a future that raises why once the worker has finished, or with `wait=False`, once
        the stop has begun.

        Calls are refused from then on. Those in flight are cancelled, along with _run(), each time this is called with
        `interrupt`, or left to finish before _run() is (a drain, which a later interrupting stop() can cut short). On the
        worker's own thread, `wait` is an error, as it would wait for itself, and interrupting cancels the calling task
        too."""

        def begin():  # On the worker's loop. Not a call, so it isn't refused once the worker has stopped: it can cut a drain short.
            if interrupt and self._calls:
                self._stopped.unclean_stop("manually stopped", self._calls)
            else:
                self._stopped.clean_stop("stopped", self._calls)
            if interrupt or not self._calls:
                self._interrupt(interrupt)  # Otherwise the last call to end does: see _job()

        if self.in_worker:
            assert not wait, "stop(wait=True) on the worker's own thread would wait for itself"
            begin()  # Interrupting cancels the calling task as it does any other: see _interrupt()
            return self._stopped
        if not self._dead_or_forked():  # Which fails both futures, in a forked process _after_fork() has somehow missed
            with suppress(RuntimeError):  # The loop has closed, so the worker has already stopped
                self._loop.call_soon_threadsafe(begin)
        return self._finished if wait else self._stopped

    def _start(self, future: AwaitableFuture, func, args, kwargs):
        # Runs a call in a task of its own, which cancelling the future cancels. Not if it's done already: cancelled by its
        # caller, or failed, in a process forked from a call, whose copy of the loop runs on.
        if future.done():
            return
        job = self._job(func, args, kwargs, future)
        job.__qualname__ += f"[{func.__qualname__}]"  # Only shown, in reprs, so needn't make an identifier
        task = self._loop.create_task(job)

        def cancel():
            # Not once the worker has interrupted it: an interrupt also cancelling what awaits it (_run()'s TaskGroup
            # cancelling its children, say) mustn't cancel it twice.
            if not (self._interrupted and task.cancelling()):
                task.cancel()

        def forward(_):
            if future.cancelled():
                with suppress(RuntimeError):  # The loop has closed
                    self._loop.call_soon_threadsafe(cancel)

        future.add_done_callback(forward)

    def _fail(self, future: concurrent.futures.Future, error: BaseException | None = None) -> bool:
        # Whether it took `error` (by default, why the worker stopped): not if it finished after all, or its caller
        # cancelled it, or it has been failed already
        try:
            future.set_exception(error or self._stopped.exception())
            return True
        except concurrent.futures.InvalidStateError:
            return False

    def _dead_or_forked(self) -> bool:
        # Whether the worker's thread can't run anything from here, recording the stop, and failing _finished, if so: a
        # backstop for
        # _after_fork(), relying as it does on Python's handling of the fork. Either the thread has finished (which, in
        # the worker's own process, it does only after _main() has recorded the stop, and stopped taking submissions), or
        # this is a process forked from the worker's. There, Python marks every thread finished but the forking one,
        # which it makes the main thread: so the worker's thread is that one only if a call on it forked.
        if dead := not self._thread.is_alive() or self._thread is main_thread():
            self._stopped.unclean_stop(f"forked into process {os.getpid()}", self._calls)
            self._fail(self._finished)
        return dead

    @classmethod
    def _after_fork(cls):
        # TODO update comments here and in finalize
        # Run by Python in a process forked (through it) from one with workers, whose copies of them have no thread to run
        # anything, even if a call forked: the child then runs on in a copy of the worker's loop, but one asyncio no
        # longer counts as running there. So it stops each, and fails what _main() would as it finished: the calls in
        # flight (leaving their coroutines alone, as closing those would run cleanup meant for the parent), and _finished.
        # Nothing in the child need then wait for a worker, unless on its loop. Workers that have finished are unaffected.
        for worker in cls._WORKERS:
            if worker._submitting.locked():
                worker._submitting.release()  # Held as the process forked, by a thread that isn't in this one
            worker._dead_or_forked()  # Set the stop exception
            worker._finalize()  # Close any job handles that might hang if waited on

    def _finalize(self):
        # Wait for any jobs that were being submitted at the moment of shutdown to finish being submitted. Because
        # _stopped has been set already everywhere this method is called, serializing here ensures that any submits
        # after this point in time will observe stopped.done and fail.
        with self._submitting:
            pending = list(self._pending)
        for future in pending:
            self._fail(future)  # Never run, or abandoned (or here, in a forked process)

    def _main(self):
        loop = self._loop
        try:
            try:
                loop.run_until_complete(self._runner)
            except (Exception, asyncio.CancelledError) as ex:
                # If a stop came first, it has recorded why already, so this does nothing. Otherwise, _run() raised
                # (CancelledError included) of its own accord.
                stopping = self._stopped.unclean_stop(f"_run() raised {ex!r}", self._calls, ex)
            else:
                stopping = self._stopped.clean_stop("_run() returned", self._calls)
            if stopping:
                self._interrupt(True)  # _run() ending was the stop, so it interrupts calls, as stop(interrupt=True) would
            # Let calls still in flight finish (or, interrupted, clean up): the teardown would cancel them.
            if self._calls:
                # Only wait if we have any; asyncio.wait raises on empty
                loop.run_until_complete(asyncio.wait(list(self._calls)))
        except BaseException as exc:
            # Should never happen, except for a KeyboardInterrupt or SystemExit from _run() or a call: asyncio lets
            # those escape the loop from any task. Record the crash (logging it, if it's the first stop), so that calls
            # refused from now on say why.
            self._stopped.unclean_stop("the worker's event loop crashed", self._calls, exc)
            raise
        finally:
            try:
                # TODO log the non-call/non-run tasks this cancels
                tasks = asyncio.all_tasks(loop)
                for task in tasks:
                    if task.cancel():
                        self._interrupted = True  # Calls among them (after a crash) tell their waiters why: see _job()
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
                self._fail(self._finished)  # The very last action: stop()'s waiters wait for it, and on_shutdown() follows

    def _interrupt(self, again: bool):
        # Once the worker has stopped: cancels _run() and every call in flight. Each time with `again` (for
        # stop(interrupt=True), expiry, and _run() ending of its own accord); otherwise (for a stop that doesn't interrupt,
        # once no calls are left in flight) only if the worker hasn't interrupted anything yet.
        if again or not self._interrupted:
            for task in {self._runner, *self._calls}:
                if task.cancel():
                    self._interrupted = True

    async def _job(self, func, args, kwargs, future: AwaitableFuture):
        # Runs a call (see _start()), and settles its future. It raises only crashes, and what the future can't take
        # (cancelled by its caller, say), which asyncio then logs, if it's an error, as for any task.
        task = asyncio.current_task(self._loop)
        try:
            if self._dead_or_forked() or self._stopped.done():
                raise self._stopped.exception()
            self._calls.add(task)
            self._reschedule()  # Which suspends the idle countdown
            try:
                result = await func(self, *args, **kwargs)
            finally:
                self._calls.remove(task)
                # A drain (a stop that leaves calls in flight to finish) cancels _run() only once the last has ended: here.
                # Nothing else would, unless _run() ended of its own accord, or a later stop() interrupted.
                if self._stopped.done() and not self._calls:
                    self._interrupt(False)
                self._reschedule()  # Which restarts the countdown, once no calls are left in flight
        except BaseException as ex:
            error = ex  # Raised by func itself, if a CancelledError, as its task isn't being cancelled
            if isinstance(ex, asyncio.CancelledError) and task.cancelling() and self._interrupted:
                # Its waiters get where the job was cancelled, raised from (a copy of) why the worker stopped, itself raised
                # from what caused that, if anything. (Otherwise, its caller cancelled the future.)
                error = WorkerInterrupted(*ex.args).with_traceback(ex.__traceback__)
                error.__cause__ = self._stopped.exception()
            if not self._fail(future, error) or not isinstance(ex, Exception | asyncio.CancelledError):
                raise
        else:
            with suppress(concurrent.futures.InvalidStateError):  # Its caller cancelled it
                future.set_result(result)



if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=CaptiveWorker._after_fork)
