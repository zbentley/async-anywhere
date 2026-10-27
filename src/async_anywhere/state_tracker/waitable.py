from __future__ import annotations

from abc import ABC
from threading import Lock
from typing import Any
from weakref import WeakSet

from async_anywhere._util import ThreadSelector, LoopSelector, blocking_wait_checker
from async_anywhere.futures import AwaitableFuture
from async_anywhere.state_tracker import State, StateTracker, StateTrackerError


class WaitableStateTracker(StateTracker, ABC):
    _waiters: dict[State, WeakSet[AwaitableFuture]]

    __slots__ = ('_waiters', '_runtime_checker')
    def __init__(
        self,
        prevent_blocking_wait_in_loops: LoopSelector = None,
        prevent_blocking_wait_in_threads: ThreadSelector = None,
        data: Any = None,
    ):
        """
        Always thread safe, as its futures can be waited on and resolved from any thread. Futures from wait_for_state()
        are held weakly, so ones whose callers drop them (after adding done callbacks, say) are never resolved.

        prevent_blocking_wait_in_loops, prevent_blocking_wait_in_threads: given to every AwaitableFuture that
            wait_for_state() returns; see AwaitableFuture.
        data: the initial state's, which wait_for_state() gives for it as it does a transition's.
        """
        super().__init__(lock=Lock())
        self._last_transition = (None, self.state, data)
        self._waiters = {s: WeakSet() for s in self.states}
        # Made once, for all its futures, as an iterator would only last one
        self._runtime_checker = blocking_wait_checker(prevent_blocking_wait_in_loops, prevent_blocking_wait_in_threads)

    def wait_for_state(self, state: State, *states: State) -> AwaitableFuture:
        """A future that resolves to the data of the transition into the first of `states` the tracker enters, or of the
        one it's in, if it's one of them. Once it's in a terminal state, it never transitions again: waiting for any
        other fails with StateTrackerError."""
        # NB: the future returned here will be resolved once the StateTracker has passed through the given state, even
        # if it is no longer in that state. If a worker transitions twice in rapid succession, or a waiter is attached
        # and then not waited on until later, after the worker has passed through *states, it'll still resolve.
        # NB: cancelling the returned future does nothing.
        # NB: if your done callbacks are slow, you'll break stuff (here and everywhere)
        states = [self._check_state(state) for state in (state, *states)]
        future = AwaitableFuture(honor_cancel=False)  # Giving up on it leaves it, and other waiters, alone
        future._runtime_checker = self._runtime_checker
        with self._condition:
            _, current, data = self._last_transition
            if current in states:
                future.set_result(data)
            elif current.terminal:
                future.set_exception(StateTrackerError(self, "Can never reach state(s)", *states))
            else:
                for state in states:
                    self._waiters[state].add(future)
        return future

    def _transition(self, target: State, data: Any = None):
        with self._condition:
            old_state = self.state
            if target not in old_state._destinations:
                raise StateTrackerError(self, "Could not transition to state", self._check_state(target))
            self._last_transition = (old_state, target, data)
            old_waiters = self._waiters[target]
            waiters = tuple(old_waiters)  # Not copy() because we don't need to hash -- TODO right? Or is copy faster even on weakset?
            old_waiters.clear()
            stranded = {}
            if target.terminal:  # Never to transition again, so those waiting for any other state would wait forever
                stranded, self._waiters = self._waiters, {}
            self.after_state_change(old_state, target, data)

        for future in waiters:
            future.set_result_if_not_done(data)
        for state, futures in stranded.items():
            for future in futures:  # Each its own error, as raising one adds to its traceback
                future.set_exception_if_not_done(StateTrackerError(self, "Can never reach state(s)", state))
