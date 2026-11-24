from __future__ import annotations

import asyncio
import sys
from inspect import CORO_CREATED, getcoroutinestate
from types import CoroutineType
from typing import Coroutine

from async_anywhere.futures import AwaitableFuture

# Before 3.14, task factories may not take eager_start: asyncio's own eager_task_factory doesn't
_factories_take_eager_start = sys.version_info >= (3, 14)


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
        # Not isinstance(asyncio.Task): a task factory's tasks needn't be, as asyncio.tasks._PyTask isn't
        return None if isinstance(target := self._target, CoroutineType) else target

    def start(self, loop: asyncio.AbstractEventLoop):
        # eager_start=True ensures that the job is queued to run before anything dispatched after it. Not setting this
        # leads to confusing priority semantics between jobs started inside the executor vs. outside.
        # Its first step runs in create_task(), so current_job() finds it by its coroutine meanwhile.
        try:
            if _factories_take_eager_start or loop.get_task_factory() is None:
                task = loop.create_task(self._target, eager_start=True)
            else:  # Started eagerly or not, as the factory decides
                task = loop.create_task(self._target)
        except (
            Exception
        ) as ex:  # A task factory raising, say: else its waiters hang, as asyncio only logs a callback's error
            if getcoroutinestate(job := self._target) != CORO_CREATED:  # Its first step ran, so _job() handled it
                raise
            reported = self.set_exception_if_not_done(ex)
            try:
                job.send(None)  # _job(), seeing its future done, just does its cleanup: no await, so it returns at once
            except StopIteration:
                pass
            if not reported:  # Cancelled by its caller first: asyncio logs it, as it would a task's unretrieved error
                raise
            return
        if not task.done():  # Else it ended in its first step, and was finish()ed already
            self._target = task

    def finish(self) -> bool:
        # Returns whether its job never started. Its task's context holds the executor, which a finished job's future
        # mustn't keep alive
        target, self._target = self._target, None
        # Its coroutine, never started, so never will be: not running its first step, which this ends, eagerly
        if isinstance(target, CoroutineType) and getcoroutinestate(target) == CORO_CREATED:
            target.close()  # As Python warns about coroutines dropped unawaited
            return True
        return False
