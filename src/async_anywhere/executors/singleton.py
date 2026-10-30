"""An EventLoopExecutor that's replaced once it stops (by expiry, a crash, or a fork, say), for a client that should
outlive any one worker: a database connection, for one. A stopped worker is replaced only once it has shut down, so that
only one ever runs."""
from contextvars import copy_context
from functools import partial, update_wrapper
from threading import Lock
from types import SimpleNamespace
from typing import Any, Callable, Coroutine

from async_anywhere.executors import EventLoopExecutor
from async_anywhere.executors._fork_safety import workers
from async_anywhere.executors.exceptions import WorkerStopped
from async_anywhere.futures import AwaitableFuture, chain

# What a singleton holds when it has no worker to use (before the first get(), once the constructor has raised, and in
# a forked process): a stopped one, so that get() makes one
_STOPPED = AwaitableFuture()
_STOPPED.set_result(SimpleNamespace(state=EventLoopExecutor.Stopped))  # Not a class, whose creation would rebind the State


class SingletonEventLoopExecutor:
    def __init__(self, *, lazy: bool = True, interrupt_stopping_workers: bool = True, **kwargs):
        """kwargs are EventLoopExecutor's, for each worker. With lazy=False, the first is made at once; otherwise, by
        the first get(). A stopping worker is shut down with shutdown(cancel_futures=interrupt_stopping_workers)."""
        self._kwargs = kwargs
        self._interrupt_stopping_workers = interrupt_stopping_workers
        self._lock = Lock()
        # The worker's future, pending while its predecessor shuts down, and the predecessor's Stopped future that
        # resolves it: held here, as the worker holds it only weakly
        self._future, self._stopped = _STOPPED, None
        workers.ensure_registered(self)
        if not lazy:
            self.get().result()  # Raising what the constructor does

    # TODO make this a state machine with observable, but not waitable, states: not started, running/stopping/stopped,
    # loopable back around.

    def _after_fork_in_child(self):
        self._lock.acquire(timeout=0)
        self._lock.release()
        self._future = _STOPPED  # Its worker has stopped here, or, if pending, it's been cancelled

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
                    stopped = worker.shutdown(wait=False, cancel_futures=self._interrupt_stopping_workers)
                    checker = worker._runtime_checker  # Waiting for its replacement on its own loop would deadlock
                case EventLoopExecutor.Stopped:
                    stopped, checker = _STOPPED, None  # Done already, so it's replaced at once
            workers.ensure_registered(self)  # Again, in a forked process, whose fork handler let go of it
            self._future = future = AwaitableFuture(
                prevent_blocking_wait_in_loops=checker, ignore_cancel=True, cancel_pending_in_forked_child=True
            )
            self._stopped = stopped

        def replace(_):  # Here, or on the old worker's thread, as it finishes shutting down
            try:
                worker = EventLoopExecutor(**self._kwargs)
            except Exception as ex:
                with self._lock:
                    self._future = _STOPPED  # For the next get() to try again
                future.set_exception(ex)
            else:
                future.set_result(worker)

        stopped.add_done_callback(replace)  # Outside the lock, so that other get()s don't wait for the constructor
        return future

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
