from __future__ import annotations

from abc import ABC
from operator import itemgetter
from typing import ContextManager, Literal, TypeAlias, overload
from weakref import WeakSet

from async_anywhere.exceptions import StateTrackerError
from async_anywhere.futures import AwaitableFuture
from async_anywhere.state_tracker import State, StateTracker

_data = itemgetter(2)  # A transition's data, from its (old state, new state, data) triple


class WaitableStateTracker[T](StateTracker, ABC):
    # Old state and data are None for the initial state, which no transition entered
    # Not a PEP 695 type statement, which Sphinx renders by its name: this one is transparent, so docs show the tuple
    _FutureType: TypeAlias = tuple[State | None, State, T]
    _waiters: dict[State, WeakSet[AwaitableFuture[T]]]

    __slots__ = ('_waiters',)
    def __init__(self, lock: ContextManager | None = None):
        """
        Always thread safe, as its futures can be waited on and resolved from any thread. Futures from wait_for_state()
        are held weakly, so ones whose callers drop them (after adding done callbacks, say) are never resolved.

        lock: see StateTracker. It must be a real lock, as its futures are waited on and resolved from any thread.
        """
        super().__init__(lock=lock)
        self._waiters = {s: WeakSet() for s in self.states}

    @overload
    def wait_for_state(
        self,
        state: State,
        *states: State,
        ignore_current_state: bool = False,
        return_states: Literal[False] = False,
        **kwargs,
    ) -> AwaitableFuture[T]: ...
    @overload
    def wait_for_state(
        self, state: State, *states: State, ignore_current_state: bool = False, return_states: Literal[True], **kwargs
    ) -> AwaitableFuture[_FutureType]: ...
    def wait_for_state(
        self, state: State, *states: State, ignore_current_state: bool = False, return_states: bool = False, **kwargs
    ) -> AwaitableFuture[T]:
        # NB: the future returned here will be resolved once the StateTracker has passed through the given state, even
        # if it is no longer in that state. If a worker transitions twice in rapid succession, or a waiter is attached
        # and then not waited on until later, after the worker has passed through *states, it'll still resolve.
        # NB: ignore_current_state=True always waits, for the next transition into *states, even if already in one.
        # NB: it resolves with that transition's data (None for the initial state); with return_states=True, with its
        # whole (old state, new state, data) triple.
        # NB: cancelling the returned future does nothing.
        # NB: if your done callbacks are slow, you'll break stuff (here and everywhere)
        # NB: kwargs go to AwaitableFuture() (prevent_blocking_wait_when, say), bar ignore_cancel and
        # transform_result: those are errors
        states = [self._check_state(state) for state in (state, *states)]
        future = AwaitableFuture(  # Giving up on it leaves it, and other waiters, alone
            **kwargs, ignore_cancel=True, transform_result=None if return_states else _data
        )
        with self._condition:
            if not ignore_current_state and self.state in states:
                future.set_result(self._last_transition)
            else:
                for state in states:
                    self._waiters[state].add(future)
        return future

    def _transition(self, target: State, data: T | None = None):
        with self._condition:
            old_state = self.state
            if target not in old_state._destinations:
                raise StateTrackerError(self, "Could not transition to state", self._check_state(target))
            self._last_transition = result = (old_state, target, data)
            old_waiters = self._waiters[target]
            waiters = tuple(old_waiters)  # Not copy() because we don't need to hash -- TODO right? Or is copy faster even on weakset?
            old_waiters.clear()
            self._after_state_change(old_state, target, data)

        for future in waiters:
            future.set_result_if_not_done(result)
