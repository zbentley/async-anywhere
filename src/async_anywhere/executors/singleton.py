"""An EventLoopExecutor that's replaced once it stops (by expiry, a crash, or a fork, say), for a client that should
outlive any one worker: a database connection, for one. A stopped worker is replaced only once it has shut down, so that
only one ever runs."""
from contextvars import copy_context
from functools import partial, update_wrapper
from threading import RLock, current_thread
from types import SimpleNamespace
from typing import Any, Callable, Coroutine, Literal, overload
from weakref import WeakSet

from async_anywhere._util import LockProtected, LoopSelector
from async_anywhere.exceptions import WorkerForked, WorkerStopped, WouldBlock
from async_anywhere.executors import EventLoopExecutor
from async_anywhere.executors._fork_safety import workers
from async_anywhere.futures import AwaitableFuture, chain
from async_anywhere.state_tracker import State
from async_anywhere.state_tracker.waitable import WaitableStateTracker

# What a singleton holds when it has no worker to use (before the first get(), once the constructor has raised, and in
# a forked process): a stopped one, so that get() makes one
_SHUTDOWN = AwaitableFuture()
_SHUTDOWN.set_result(SimpleNamespace(state=EventLoopExecutor.Shutdown))  # Not a class, whose creation would rebind the State


class SingletonEventLoopExecutor(WaitableStateTracker):
    # As its worker's states, which it follows, but Shutdown until get() makes one, and from Shutdown back to Running as
    # get() makes the next: that transition's data is the worker, and the others' are the worker's own
    Running = State()
    Stopping = State(sources=Running)
    Shutdown = State(sources=Stopping, destinations=Running, initial=True)

    def __init__(self, *, lazy: bool = True, interrupt_stopping_workers: bool = True, **kwargs):
        """kwargs are EventLoopExecutor's, for each worker. With lazy=False, the first is made at once; otherwise, by
        the first get(). A stopping worker is shut down with shutdown(cancel_futures=interrupt_stopping_workers)."""
        super().__init__()
        self._kwargs = kwargs
        self._interrupt_stopping_workers = interrupt_stopping_workers
        # Every worker's thread, added as it's made, on which waiting for this to move would deadlock in its loop or not
        # (as done callbacks run once it's stopped): they move it. Not self, which the futures would keep alive.
        self._threads = threads = WeakSet()
        self._runtime_checker = partial(WouldBlock._invariant, lambda _: current_thread() in threads)
        # The worker's future, pending while its predecessor shuts down, this one's Shutdown future that resolves it
        # (once this has followed the predecessor there), and the worker's state future that this follows it by: held
        # here, as their trackers hold them only weakly
        self._future, self._stopped, self._following = LockProtected(_SHUTDOWN), None, None
        self._ready = None  # The replacement's own future, while get()'s is pending: see _get()
        workers.ensure_registered(self)
        if not lazy:
            self.get().result()  # Raising what the constructor does

    def _after_fork_in_child(self):
        self._condition = RLock()  # As a worker's: a thread that isn't here may hold it
        # A new one, rather than replace_value(): a thread that isn't here may hold its lock too
        self._future = LockProtected(_SHUTDOWN)  # Its worker has stopped here, or, if pending, it's been cancelled
        # As its worker does here, whose state futures' callbacks, which this follows it by, don't run here
        forked = WorkerForked()
        self._try_transition(self.Stopping, forked)
        self._try_transition(self.Shutdown, forked)

    def get(self) -> AwaitableFuture[EventLoopExecutor]:
        """The worker's future, which can't be cancelled. If the worker has stopped, a new one's, which resolves once
        the old has shut down: shut down by the first get() to find it stopping. It fails with what the constructor
        raises (or with a WorkerStopped, if that's interrupted, by Ctrl+C, say), which the next get() tries again."""
        return self._get()[0]

    def _get(self) -> tuple[AwaitableFuture[EventLoopExecutor], AwaitableFuture[EventLoopExecutor]]:
        # get()'s future, and, while that's pending, the replacement's own (else get()'s again): only this adds
        # callbacks to that one (see submit() and shutdown()), which is resolved first, so that none users add to
        # get()'s can keep them from running
        with self._future as future:
            if not future.done():
                return future, self._ready  # A replacement, pending
            match (worker := future.result()).state:
                case EventLoopExecutor.Running:
                    return future, future
                case EventLoopExecutor.Stopping:
                    worker.shutdown(wait=False, cancel_futures=self._interrupt_stopping_workers)
                    checker = worker._runtime_checker  # Waiting for its replacement on its own thread would deadlock
                case EventLoopExecutor.Shutdown:
                    checker = None  # Done already, so it's replaced at once, unless this is yet to follow it to Shutdown
            workers.ensure_registered(self)  # Again, in a forked process, whose fork handler let go of it
            future, ready = (
                AwaitableFuture(
                    prevent_blocking_wait_in_loops=checker, ignore_cancel=True, cancel_pending_in_forked_child=True
                )
                for _ in range(2)
            )
            self._future.replace_value(future)
            self._ready = ready
            # This one's, not the worker's: so that this is Shutdown before it's Running again
            self._stopped = stopped = self.wait_for_state(self.Shutdown)

        def replace(_):  # Here, or on the old worker's thread, as it finishes shutting down
            try:
                worker = EventLoopExecutor(**self._kwargs)
            except BaseException as ex:  # What the constructor raises, or a KeyboardInterrupt (Ctrl+C) as it runs, say
                with self._future:
                    self._future.replace_value(_SHUTDOWN)  # For the next get() to try again
                # If interrupted, raised on here: their waiters, on any thread, get an Exception saying so
                error = ex if isinstance(ex, Exception) else WorkerStopped("interrupted while starting", ex)
                try:
                    ready.set_exception(error)
                finally:  # Whatever callbacks on the calls it fails raise
                    future.set_exception(error)
                if error is not ex:
                    raise
            else:
                self._threads.add(worker._thread)

                def stopping(begun):  # On the worker's thread as it stops, unless it had already
                    self._transition(self.Stopping, begun.result())
                    self._following = worker.wait_for_state(worker.Shutdown)
                    self._following.add_done_callback(lambda done: self._transition(self.Shutdown, done.result()))

                try:
                    self._transition(self.Running, worker)
                finally:  # Whatever its waiters' callbacks raise: else nothing follows the worker, and get() waits for good
                    # Following the worker to Stopping, then from there to Shutdown: both at once, if it's stopped already
                    self._following = worker.wait_for_state(worker.Stopping, worker.Shutdown)
                    self._following.add_done_callback(stopping)
                    try:
                        ready.set_result(worker)
                    finally:
                        future.set_result(worker)

        stopped.add_done_callback(replace)  # Outside the lock, so that other get()s don't wait for the constructor
        return future, ready

    def wait_for_state(
        self, state: State, *states: State, prevent_blocking_wait_in_loops: LoopSelector = None, **kwargs
    ) -> AwaitableFuture[Any]:
        """As EventLoopExecutor.wait_for_state(), for every worker's thread: one being replaced included."""
        # Refusing blocking waits on its workers' threads too, whatever's given: they move it
        on_their_threads = self._runtime_checker  # Its one checker, rather than a new one per future
        given = None if prevent_blocking_wait_in_loops is None else WouldBlock._checker(prevent_blocking_wait_in_loops)
        if given is None:  # No loops given
            checker = on_their_threads
        else:
            def checker(future):
                on_their_threads(future)
                given(future)
        return super().wait_for_state(state, *states, prevent_blocking_wait_in_loops=checker, **kwargs)

    def shutdown(
        self, wait: bool = True, *, cancel_futures: bool = False
    ) -> AwaitableFuture[WorkerStopped | None]:
        """As EventLoopExecutor.shutdown(), for get()'s worker, but its future resolves once this has followed the
        worker to Shutdown (with None, if it's yet to make one): and not for good, as the next get() makes a new worker. One still being made, to replace a
        worker that stopped, is shut down once it's made, after the calls made to it before this."""
        with self._future as future:
            worker = future.result() if future.done() else None
            if worker is not None:  # Before a get() could replace the worker, so that it's this one's stop waited for
                stopped = self.wait_for_state(self.Shutdown)
            ready = self._ready
        if worker is None:  # A replacement, pending
            if wait and future._runtime_checker is not None:
                future._runtime_checker(future)  # Refusing to wait where it would deadlock, before stopping anything
            stopped = AwaitableFuture(
                prevent_blocking_wait_in_loops=future._runtime_checker,
                ignore_cancel=True,
                cancel_pending_in_forked_child=True,
            )

            def stop(made):  # Once the calls made to the replacement before this have started: see _get()
                if made.exception() is None:
                    made.result().shutdown(wait=False, cancel_futures=cancel_futures)
                # Linked both ways, so that stopped holds the future that resolves it, which this holds only weakly
                chain.link_futures(self.wait_for_state(self.Shutdown), stopped)

            ready.add_done_callback(stop)
        elif future is not _SHUTDOWN:
            worker.shutdown(wait=wait, cancel_futures=cancel_futures)
        if wait:
            stopped.result()
        return stopped

    def submit[**P, T](
        self, fn: Callable[P, Coroutine[Any, Any, T]], /, *args: P.args, **kwargs: P.kwargs
    ) -> AwaitableFuture[T]:
        """As EventLoopExecutor.submit(), to get()'s worker: once it's been made, if it's being replaced. If the worker
        stopped since get(), refusing the call, it's made once more, to its replacement."""
        pending, ready = self._get()
        if pending.done():  # Its worker's running, unless the constructor raised, which this raises
            try:
                return pending.result().submit(fn, *args, **kwargs)
            except WorkerStopped:  # It stopped since get()
                pending, ready = self._get()
        # Refusing blocking waits wherever waiting for the replacement would deadlock
        call = AwaitableFuture(
            prevent_blocking_wait_in_loops=pending._runtime_checker, cancel_pending_in_forked_child=True
        )
        context = copy_context()  # The caller's, for the job to copy, as it would submitting here
        job = None

        def cancel_job(_):
            if call.cancelled() and job is not None:
                job.cancel()

        def start(made):  # Before any callbacks users added to get()'s future: see _get()
            nonlocal job
            if call.done():  # Cancelled meanwhile
                return
            try:
                job = context.run(made.result().submit, fn, *args, **kwargs)
            except Exception as ex:  # The constructor raised, or the replacement refused the call
                call.set_exception_if_not_done(ex)
            else:
                chain.link_futures(job, call, on_dest_cancel=chain.DO_NOTHING)  # Its result: see cancel_job()
                if call.cancelled():  # Since done() above, so before cancel_job() could find the job
                    job.cancel()

        # Its first callback, as no caller has it yet, so that none of theirs can keep it from running (by raising a
        # KeyboardInterrupt from Ctrl+C, say)
        call.add_done_callback(cancel_job)
        ready.add_done_callback(start)
        return call

    def bridge[**P, T](self, func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, AwaitableFuture[T]]:
        """As EventLoopExecutor.bridge(), its calls going through submit() above."""
        return update_wrapper(partial(self.submit, func), func)
