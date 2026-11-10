import asyncio
from inspect import CORO_CREATED, getcoroutinestate
from typing import Coroutine

from async_anywhere.futures import AwaitableFuture


class _ExecutorJob(AwaitableFuture):
    """A job's future, as EventLoopExecutor.submit() returns it: documented as just an AwaitableFuture. No constructor
    of its own, as submit() is a hot path: submit() sets _target itself."""

    __slots__ = ("_target",)

    # Set by executor.submit(), the only place these are constructed (to avoid call overhead due to a subclass constructor
    # on a hot path). Starts out set to the not-yet-run coroutine object, and converted to a started Task when start()
    # is called:
    _target: Coroutine | asyncio.Task | None

    @property
    def task(self) -> asyncio.Task | None:
        """Its job's task, once started, until finished. Only on the executor's loop."""
        return target if isinstance(target := self._target, asyncio.Task) else None

    def start(self, loop: asyncio.AbstractEventLoop):
        # eager_start=True ensures that the job is queued to run before anything dispatched after it. Not setting this
        # leads to confusing priority semantics between jobs started inside the executor vs. outside.
        # Its first step runs in create_task(), so current_job() finds it by its coroutine meanwhile.
        task = loop.create_task(self._target, eager_start=True)
        if not task.done():  # Else it ended in its first step, and was finish()ed already
            self._target = task

    def finish(self):
        # Its task's context holds the executor, which a finished job's future mustn't keep alive
        target, self._target = self._target, None
        # Its coroutine, never started, so never will be: not running its first step, which this ends, eagerly
        if target is not None and not isinstance(target, asyncio.Task) and getcoroutinestate(target) == CORO_CREATED:
            target.close()  # As Python warns about coroutines dropped unawaited
