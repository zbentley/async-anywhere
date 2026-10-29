from __future__ import annotations

import inspect
from abc import ABC
from collections import defaultdict
from threading import RLock
from typing import Any, ClassVar, Self, ContextManager

from async_anywhere.state_tracker._exceptions import StateTrackerError
from async_anywhere.state_tracker._state import State, _binding_lock, _structural_invariant

__all__ = ["State", "StateTracker", "StateTrackerError"]


# TODO think about hash or not
class StateTracker(ABC):
    """
    A class which has a field, ".state", containing a State object representing the current state in a state machine/
    state transition graph of this class.

    Users are intended to subclass StateTracker for their own use-cases. When subclassing StateTracker, users should
    declare one or more State objects as class-level fields, like this:

    >>> class MyStateTracker(StateTracker):
    >>>    InitialState = State(initial=True)
    >>>    OtherState = State(sources=[InitialState])
    >>>    ...

    When this is done, instances of MyStateTracker will start with "inst.state == MyStateTracker.InitialState".

    States must be declared together on one class; hey cannot be combined or layered across subclasses. States must be
    declared in one of the following locations:

    1. On a concrete StateTracker subclass, as explicit, literal class fields (not via descriptors or __class_getitem__).
    2. On any class that is inherited by a StateTracker subclass as explicit, literal fields. This allows for common
       state graphs to be used by multiple different StateTracker subclasses via a mixin.

    All concrete StateTracker subclasses must declare States. Abstract StateTracker subclasses must be extended by a
    concrete class which declares States.

    Note: if using `attrs`, `dataclass` or other class-creation factories, all State objects declared at the class level
    should be annotated with ClassVar, otherwise subclass declaration may fail.

    Users can call StateTracker._transition() to move a StateTracker between states (or _try_transition(), which returns
    whether the transition was allowed, rather than raising StateTrackerError if not).

    Note that the "_transition()" method *is* a user-callable public API. It is declared as _private for convenience,
    since many uses of StateTracker may not want the state to be externally transitionable. You can declare a public
    transition method via code like:

    >>> class PubliclyTransitionableStateTracker(StateTracker):
    >>>      transition = StateTracker._transition  # or WaitableStateTracker._transition
    >>>      ...
    """

    states: ClassVar[frozenset[State]]
    _abstract: ClassVar[bool]
    _initial_transition: ClassVar[tuple[None, State | None, None]]

    # (old state, new state, data) of the last transition, or (None, initial state, None) before any: one tuple, so that
    # lock-free readers always see a state and its data together
    _last_transition: tuple[State | None, State, Any]

    __slots__ = ('_last_transition', '_condition')

    def __init__(self, lock: ContextManager | None = None) -> None:
        """
        lock: held by each transition, and its after_state_change(). The default is a fresh RLock, so
            after_state_change() can use the StateTracker again. Pass contextlib.nullcontext() to take no lock, and only
            use the StateTracker from one thread at a time.
        """
        self._last_transition = self._initial_transition
        self._condition = RLock() if lock is None else lock

    # We use a property to prevent accidental manual mutation
    @property
    def state(self) -> State:
        return self._last_transition[1]

    @classmethod
    def _check_states(cls, seen: frozenset[State], source, *states):
        # TODO warn unreachable/prime DAG?
        for state in states:
            if state is not Any and state is not Self:
                _structural_invariant(
                    cls,
                    state in seen,
                    f'State "{source}" has a source or destination state that is not part of this state tracker: {state}'
                )

        if Any in states:
            yield from seen
        else:
            for state in states:
                if state is Self:
                    yield source
                else:
                    yield state

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Class builders that add slots rerun this on a copy of the namespace, where inspect.isabstract() is wrong
        if (is_abstract := cls.__dict__.get("_abstract")) is None:
            is_abstract = cls._abstract = ABC in cls.__bases__ or inspect.isabstract(cls)
        declarers = [k for k in cls.__mro__ if any(isinstance(v, State) for v in vars(k).values())]
        if len(declarers) == 1:
            states =  frozenset(v for v in vars(declarers[0]).values() if isinstance(v, State))
        else:
            _structural_invariant(
                cls,
                len(declarers) == 0,
                f"States must all be declared on one class, but are declared on {', '.join(k.__qualname__ for k in declarers)}"
            )
            states = frozenset()
        destinations = defaultdict(set)
        initial = None
        for state in states:
            _structural_invariant(
                cls,
                inspect.getattr_static(cls, state.name, None) is state,
                f"{cls.__qualname__}.{state.name} is not {state!r}; States can't be shadowed, or added to a class after it's declared"
            )
            if state.initial:
                _structural_invariant(cls, initial is None, f"Multiple states with initial=True: '{state}' and '{initial}'")
                initial = state

            destinations[state].update(cls._check_states(states, state, *state._destinations))
            for source in cls._check_states(states, state, *state._sources):
                destinations[source].add(state)

        _structural_invariant(
            cls,
            initial is not None or (is_abstract and len(states) == 0),
            "No states were declared with initial=True; one and only one initial state is required"
        )
        with _binding_lock:
            for state in states:
                # The same for every class sharing these States, so rewriting it for each is harmless
                dests = frozenset(destinations[state])
                object.__setattr__(state, "_destinations", dests)
                if len(dests) == 0:
                    object.__setattr__(state, "terminal", True)
                # TODO wipe sources?
        cls.states = states
        cls._initial_transition = (None, initial, None)  # A tuple, not a State, so this class doesn't declare one

    def after_state_change(self, old_state: State, new_state: State, data: Any):
        """To be overridden by subclassers."""
        # NB: runs under the lock, just after the transition, so self.state is new_state; keep it brief

    def _check_state(self, state: State) -> State:
        if state not in self.states:
            err = StateTrackerError(
                self,
            f"State '{state.name}' is not one of this class's states. Valid states:",
            *self.states)
            raise TypeError(err.args[0])
        return state

    def _transition(self, target: State, data: Any = None):
        with self._condition:
            old_state = self._last_transition[1]
            if target not in old_state._destinations:
                raise StateTrackerError(self, "Could not transition to state", self._check_state(target))
            self._last_transition = (old_state, target, data)
            self.after_state_change(old_state, target, data)

    def _try_transition(self, target: State, data: Any = None) -> bool:
        # Through _transition(), so a subclass's is used. A State not of this class still raises TypeError.
        try:
            self._transition(target, data)
        except StateTrackerError:
            return False
        return True

    def require(self, state: State, *desired_states: State) -> Self:
        current_state = self.state
        if current_state is state or current_state in desired_states:
            return self
        # Validate the inputs on the failure path so the happy path stays fast:
        for s in (state, *desired_states):
            self._check_state(s)
        raise StateTrackerError(self, "Not in required state(s)", state, *desired_states)

