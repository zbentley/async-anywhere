"""An EventLoopExecutor that's replaced once it stops (by expiry, a crash, or a fork, say), for a client that should
outlive any one worker: a database connection, for one. A stopped worker is replaced only once it has shut down, so that
only one ever runs."""
from contextvars import copy_context
from functools import partial, update_wrapper
from threading import Lock, RLock
from types import SimpleNamespace
from typing import Any, Callable, Coroutine
from weakref import WeakSet

from async_anywhere._util import LoopSelector
from async_anywhere.exceptions import WorkerForked, WorkerStopped, WouldBlock
from async_anywhere.executors import EventLoopExecutor
from async_anywhere.executors._fork_safety import workers
from async_anywhere.futures import AwaitableFuture, chain
from async_anywhere.state_tracker import State
from async_anywhere.state_tracker.waitable import WaitableStateTracker

# What a singleton holds when it has no worker to use (before the first get(), once the constructor has raised, and in
# a forked process): a stopped one, so that get() makes one
_STOPPED = AwaitableFuture()
_STOPPED.set_result(SimpleNamespace(state=EventLoopExecutor.Stopped))  # Not a class, whose creation would rebind the State


class SingletonEventLoopExecutor(WaitableStateTracker):
    # As its worker's states, which it follows, but Stopped until get() makes one, and from Stopped back to Running as
    # get() makes the next: that transition's data is the worker, and the others' are the worker's own
    Running = State()
    Stopping = State(sources=Running)
    Stopped = State(sources=Stopping, destinations=Running, initial=True)

    def __init__(self, *, lazy: bool = True, interrupt_stopping_workers: bool = True, **kwargs):
        """kwargs are EventLoopExecutor's, for each worker. With lazy=False, the first is made at once; otherwise, by
        the first get(). A stopping worker is shut down with shutdown(cancel_futures=interrupt_stopping_workers)."""
        super().__init__()
        self._kwargs = kwargs
        self._interrupt_stopping_workers = interrupt_stopping_workers
        self._lock = Lock()
        # Every worker's loop, added as it's made, on which waiting for this to move would deadlock: their threads move it
        self._loops = WeakSet()
        self._runtime_checker = partial(WouldBlock._invariant, self._loops.__contains__)
        # The worker's future, pending while its predecessor shuts down, this one's Stopped future that resolves it
        # (once this has followed the predecessor there), and the worker's state future that this follows it by: held
        # here, as their trackers hold them only weakly
        self._future, self._stopped, self._following = _STOPPED, None, None
        workers.ensure_registered(self)
        if not lazy:
            self.get().result()  # Raising what the constructor does

    def _after_fork_in_child(self):
        self._lock.acquire(timeout=0)
        self._lock.release()
        self._condition = RLock()  # As a worker's: a thread that isn't here may hold it
        self._future = _STOPPED  # Its worker has stopped here, or, if pending, it's been cancelled
        # As its worker does here, whose state futures' callbacks, which this follows it by, don't run here
        forked = WorkerForked()
        self._try_transition(self.Stopping, forked)
        self._try_transition(self.Stopped, forked)

    def get(self) -> AwaitableFuture[EventLoopExecutor]:
        """The worker's future, which can't be cancelled. If the worker has stopped, a new one's, which resolves once
        the old has shut down: shut down by the first get() to find it stopping. It fails with what the constructor
        raises, which the next get() tries again."""
        with self._lock:
            future = self._future
            if not future.done():
                return future  # A replacement, pending
            match (worker := future.result()).state:
                case EventLoopExecutor.Running:
                    return future
                case EventLoopExecutor.Stopping:
                    worker.shutdown(wait=False, cancel_futures=self._interrupt_stopping_workers)
                    checker = worker._runtime_checker  # Waiting for its replacement on its own loop would deadlock
                case EventLoopExecutor.Stopped:
                    checker = None  # Done already, so it's replaced at once, unless this is yet to follow it to Stopped
            workers.ensure_registered(self)  # Again, in a forked process, whose fork handler let go of it
            self._future = future = AwaitableFuture(
                prevent_blocking_wait_in_loops=checker, ignore_cancel=True, cancel_pending_in_forked_child=True
            )
            # This one's, not the worker's: so that this is Stopped before it's Running again
            self._stopped = stopped = self.wait_for_state(self.Stopped)

        def replace(_):  # Here, or on the old worker's thread, as it finishes shutting down
            try:
                worker = EventLoopExecutor(**self._kwargs)
            except Exception as ex:
                with self._lock:
                    self._future = _STOPPED  # For the next get() to try again
                future.set_exception(ex)
            else:
                self._loops.add(worker._loop)
                self._transition(self.Running, worker)

                def stopping(begun):  # On the worker's thread as it stops, unless it had already
                    self._transition(self.Stopping, begun.result()[2])
                    self._following = worker.wait_for_state(worker.Stopped)
                    self._following.add_done_callback(lambda done: self._transition(self.Stopped, done.result()[2]))

                # Following the worker to Stopping, then from there to Stopped: both at once, if it's stopped already
                self._following = worker.wait_for_state(worker.Stopping, worker.Stopped)
                self._following.add_done_callback(stopping)
                future.set_result(worker)

        stopped.add_done_callback(replace)  # Outside the lock, so that other get()s don't wait for the constructor
        return future

    def wait_for_state(
        self, state: State, *states: State, prevent_blocking_wait_in_loops: LoopSelector = None, **kwargs
    ) -> AwaitableFuture[WaitableStateTracker._FutureType]:
        """As EventLoopExecutor.wait_for_state(), for every worker's loop: one being replaced included."""
        if prevent_blocking_wait_in_loops is None:
            prevent_blocking_wait_in_loops = self._runtime_checker  # Its one checker, rather than a new one per future
        elif prevent_blocking_wait_in_loops != "all":  # Which covers the workers' loops already
            # Held weakly, as WouldBlock._checker() holds them. Not self, which the future would keep alive.
            loops, given = self._loops, WeakSet(prevent_blocking_wait_in_loops)
            prevent_blocking_wait_in_loops = partial(WouldBlock._invariant, lambda loop: loop in loops or loop in given)
        return super().wait_for_state(state, *states, prevent_blocking_wait_in_loops=prevent_blocking_wait_in_loops, **kwargs)

    def shutdown(
        self, wait: bool = True, *, cancel_futures: bool = False
    ) -> AwaitableFuture[WaitableStateTracker._FutureType]:
        """As EventLoopExecutor.shutdown(), for get()'s worker, but its future resolves once this has followed the
        worker to Stopped: and not for good, as the next get() makes a new worker. One still being made, to replace a
        worker that stopped, is shut down once it's made, after the calls made to it before this."""
        with self._lock:
            future = self._future
            worker = future.result() if future.done() else None
            if worker is not None:  # Before a get() could replace the worker, so that it's this one's stop waited for
                stopped = self.wait_for_state(self.Stopped)
        if worker is None:  # A replacement, pending
            if wait and future._runtime_checker is not None:
                future._runtime_checker(future)  # Refusing to wait where it would deadlock, before stopping anything
            stopped = AwaitableFuture(
                prevent_blocking_wait_in_loops=future._runtime_checker,
                ignore_cancel=True,
                cancel_pending_in_forked_child=True,
            )
            # Linked both ways, so that stopped holds the future that resolves it, which this holds only weakly
            future.add_done_callback(
                lambda _: chain.link_futures(self.shutdown(wait=False, cancel_futures=cancel_futures), stopped)
            )
        elif future is not _STOPPED:
            worker.shutdown(wait=wait, cancel_futures=cancel_futures)
        if wait:
            stopped.result()
        return stopped

    def submit[**P, T](
        self, fn: Callable[P, Coroutine[Any, Any, T]], /, *args: P.args, **kwargs: P.kwargs
    ) -> AwaitableFuture[T]:
        """As EventLoopExecutor.submit(), to get()'s worker: once it's been made, if it's being replaced. If the worker
        stopped since get(), refusing the call, it's made once more, to its replacement."""
        pending = self.get()
        if pending.done():  # Its worker's running, unless the constructor raised, which this raises
            try:
                return pending.result().submit(fn, *args, **kwargs)
            except WorkerStopped:  # It stopped since get()
                pending = self.get()
        # Refusing blocking waits wherever waiting for the replacement would deadlock
        call = AwaitableFuture(
            prevent_blocking_wait_in_loops=pending._runtime_checker, cancel_pending_in_forked_child=True
        )
        context = copy_context()  # The caller's, for the job to copy, as it would submitting here

        def start(_):
            if call.done():  # Cancelled meanwhile
                return
            try:
                job = context.run(pending.result().submit, fn, *args, **kwargs)
            except Exception as ex:  # The constructor raised, or the replacement refused the call
                call.set_exception_if_not_done(ex)
            else:
                chain.link_futures(job, call)  # Cancelling call cancels the job

        pending.add_done_callback(start)
        return call

    def bridge[**P, T](self, func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, AwaitableFuture[T]]:
        """As EventLoopExecutor.bridge(), its calls going through submit() above."""
        return update_wrapper(partial(self.submit, func), func)
