from __future__ import annotations

from abc import ABC
from operator import itemgetter
from typing import Generic, Literal, TypeVar, overload, final
from weakref import WeakSet

from async_anywhere.exceptions import StateTrackerError
from async_anywhere.futures import AwaitableFuture
from async_anywhere.state_tracker import State, StateTracker

_data = itemgetter(2)  # A transition's data, from its (old state, new state, data) triple
T = TypeVar("T")
# Old state and data are None for the initial state, which no transition entered

class WaitableStateTracker(StateTracker, ABC, Generic[T]):
    """A :py:class:`StateTracker` which can be waited on, synchronously or asynchronously, via :py:func:`wait_for_state`.

    Like :py:class:`StateTracker`, this class is intended to be subclassed by users who attach one or more
    :py:class:`State` objects to it.

    Example:

        >>> class MyTracker(WaitableStateTracker):
        >>>     First = State(initial=True)
        >>>     Second = State(soruces=[First])
        >>>     Third = State(sources=[Second])

        >>> t = MyTracker()
        >>>
        >>> # Transition can occur in another thread or event loop
        >>> t._transition(MyTracker.Second, "foo")
        >>>
        >>> # Asynchronously wait it to enter Second
        >>> assert "foo" == await t.wait_for_state(MyTracker.Second)
        >>>
        >>> # Wait for either of two states
        >>> await t.wait_for_state(MyTracker.Seconds, MyTracker.Third)
        >>>
        >>> # Wait for results synchronously, with a timeout
        >>> fut = t.wait_for_state(MyTracker.Third)
        >>> fut.result(timeout=1)
    """
    __slots__ = ('_waiters',)

    def __init__(self):
        """
        Takes no parameters. Subclass instances must call this method (e.g. via ``super()``) during initialization.

        Note:
            Unlike its parent class ``StateTracker``, this class does not accept a ``thread_safe`` parameter. All
            accesses to this class are always thread-safe.
        """
        super().__init__()
        self._waiters: dict[State, WeakSet[AwaitableFuture[T]]] = {s: WeakSet() for s in self.states}

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
    ) -> AwaitableFuture[tuple[State | None, State, T]]: ...
    @final
    def wait_for_state(
        self, state: State, *states: State, ignore_current_state: bool = False, return_states: bool = False, **kwargs
    ) -> AwaitableFuture[T]:
        r"""
        Wait for the invocant ``WaitableStateTracker`` to enter one or more states. Returns a future that can be
        blocked on with a timeout or ``await``\ ed on from ``async`` code.

        Args:
            state: Treated like part of ``states``.
            *states: One or more States to wait for. If supplied State(s) are not available on the invocant's
                class, an error will be raised.
            ignore_current_state: If True, and the invocant is already in one of the requested ``states``, the returned
                future will *not* be resolved. Instead, it will be resolved the next time the invocant transitions into
                one of the requested ``states``.

                Note:
                    If the invocant has a self-referential state (e.g. ``MyState = State(sources=[typing.Self])``), and
                    that state is supplied in ``states``, the returned future will be resolved when the state tracker
                    transitions into ``MyState`` from any other state *including itself*.

            return_states:
                If True, the returned future is resolved with a 3-tuple with the same contents as
                :py:obj:`StateTracker.last_transition`: ``(old_state, new_state, data)``. If False, the returned future is resolved
                with the ``data`` argument from the call to :py:meth:`_transition` that caused the tracker to enter one
                of ``states``

            **kwargs: Any additional keyword arguments to pass :py:class:`AwaitableFuture`\ s constructor when
                constructing the return value.

                Note:
                    The ``ignore_cancel``, ``transform_result``, and ``done_callbacks`` arguments to
                    ``AwaitableFuture``\ 's constructor are `not` accepted.
        Returns:
            An :py:class:`AwaitableFuture` that will be automatically resolved when the invocant transitions into one of
            ``states``. The future will be resolved with either the data payload to :py:func:`_transition` or the value
            of :py:attr:`last_transition`, depending on ``return_states=``.

            If the invocant is already in one of ``states`` and ``ignore_current_state`` is not set, the returned
            future will be completed.

            Cancelling the returned future has no effect. Completing the returned future some other way is allowed, in
            which case it will not be completed by the state tracker.
        """
        # NB: the future returned here will be resolved once the StateTracker has passed through the given state, even
        # if it is no longer in that state. If an executor transitions twice in rapid succession, or a waiter is attached
        # and then not waited on until later, after the executor has passed through *states, it'll still resolve.
        # NB: ignore_current_state=True always waits, for the next transition into *states, even if already in one.
        # NB: it resolves with that transition's data (None for the initial state); with return_states=True, with its
        # whole (old state, new state, data) triple.
        # NB: cancelling the returned future does nothing.
        # NB: if your done callbacks are slow, you'll break stuff (here and everywhere)
        # NB: kwargs go to AwaitableFuture() (prevent_blocking_wait_in, say), bar ignore_cancel,
        # transform_result and done_callbacks: those are errors
        states = [self._check_state(state) for state in (state, *states)]
        future = AwaitableFuture(  # Giving up on it leaves it, and other waiters, alone
            # No done_callbacks: resolved below under the lock, they'd run under it too
            **kwargs, ignore_cancel=True, transform_result=None if return_states else _data, done_callbacks=None
        )
        with self._condition:
            if not ignore_current_state and self.state in states:
                future.set_result(self.last_transition)
            else:
                for state in states:
                    self._waiters[state].add(future)
        return future

    @final
    def _transition(self, target: State, data: T | None = None) -> State:
        with self._condition:
            old_state = self.state
            if target not in old_state._destinations:
                raise StateTrackerError(self, "Could not transition to state", self._check_state(target))
            self._last_transition = result = (old_state, target, data)
            old_waiters = self._waiters[target]
            waiters = tuple(old_waiters)  # Not copy() because we don't need to hash
            old_waiters.clear()
            self._after_state_change(old_state, target, data)

        for future in waiters:
            future.set_result_if_not_done(result)
        return old_state
