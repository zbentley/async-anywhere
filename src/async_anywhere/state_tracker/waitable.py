from __future__ import annotations

import asyncio
import concurrent.futures
from abc import ABC
from typing import Any
from weakref import WeakSet

from async_anywhere.futures import AwaitableFuture
from async_anywhere.state_tracker import State, StateTracker, StateTrackerError

__all__ = ["WaitableStateTracker"]


class WaitableStateTracker(StateTracker, ABC):
    # Old state and data are None for the initial state, which no transition entered
    type _FutureType = tuple[State | None, State, Any]
    _waiters: dict[State, WeakSet[AwaitableFuture[_FutureType]] | set[AwaitableFuture[_FutureType]]]

    __slots__ = ('_waiters', '_forget', '_last_transition')
    def __init__(self, strong_waiters: bool = False):
        """
        Always thread safe, as its futures can be waited on and resolved from any thread.

        strong_waiters: if True, futures from wait_for_state() are held until they're done (resolved or cancelled), so
            ones whose callers drop them (after adding done callbacks, say) still resolve. If False, they're held weakly.
        """
        super().__init__()
        self._waiters = {s: set() if strong_waiters else WeakSet() for s in self.states}
        self._last_transition = [None, None]
        forget = bool
        if strong_waiters:
            condition, waiters = self._condition, self._waiters
            def forget(done):
                with condition:
                    for state_waiters in waiters.values():
                        state_waiters.discard(done)
        self._forget = forget

    def wait_for_state(
        self, state: State, *states: State, **kwargs,
    ) -> AwaitableFuture[_FutureType]:
        # NB: the future returned here will be resolved once the StateTracker has passed through the given state, even
        # if it is no longer in that state. If a worker transitions twice in rapid succession, or a waiter is attached
        # and then not waited on until later, after the worker has passed through *states, it'll still resolve.
        # NB: cancelling the returned future does nothing.
        # NB: if your done callbacks are slow, you'll break stuff (here and everywhere)
        states = [self._check_state(state) for state in (state, *states)]
        future = AwaitableFuture(**kwargs)
        with self._condition:
            if self._state in states:
                future.set_result((self._last_transition[0], self._state, self._last_transition[1]))
                return future
            for state in states:
                self._waiters[state].add(future)
        future.add_done_callback(self._forget)
        return future

    def _transition(self, target: State, data: Any = None):
        with self._condition:
            old_state = self._state
            if target not in old_state._destinations:
                raise StateTrackerError(self, "Could not transition to state", self._check_state(target))
            old_waiters, self._state = self._waiters[target], target
            waiters = tuple(old_waiters)  # Not copy() because we don't need to hash -- TODO right? Or is copy faster even on weakset?
            old_waiters.clear()
            self._last_transition[0] = old_state
            self._last_transition[1] = data
            # Only wakes other things trying to _transition; the futures' internals will handle waking up whatever
            # routines are waiting on them.
            self._condition.notify_all()

        result = (old_state, target, data)
        for future in waiters:
            try:
                future.set_result(result)
            except (concurrent.futures.InvalidStateError, asyncio.InvalidStateError):
                pass

        self.after_state_change(old_state, target, data)
