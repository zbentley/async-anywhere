from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import threading
from abc import ABC
from collections import defaultdict
from typing import Any, ClassVar, Iterable, Self, final
from weakref import WeakSet

import attr

from async_anywhere.futures import AwaitableFuture

# NB: in this instance, we mean "the literal class object for the typing.Self type", not "the type of the invoking class".
type Transition = State | type[Any] | type[Self]


# TODO test that superclass ctors are empty
class StateTrackerError(asyncio.InvalidStateError, concurrent.futures.InvalidStateError):
    def __init__(
        self,
        source: StateTracker,
        msg: str,
        *states: State,
    ):
        desc = self.state_names(*states)
        errprefix = f"{source.__class__.__qualname__}[state={source.state.name}]"
        super().__init__(f"{errprefix}: {msg.strip(" :")}: {desc}")

    @classmethod
    def state_names(cls, *states: State) -> str:
        assert len(states) > 0
        if len(states) == 1:
            return states[0].name
        return f'one of {", ".join(sorted(s.name for s in states))}'

def _convert_state(val: Transition | Iterable[Transition]) -> set[Transition]:
    rv = set()
    if val in (Any, Self) or isinstance(val, State):
        rv.add(val)
    else:
        for item in val:
            if not (item in (Any, Self) or isinstance(item, State)):
                raise TypeError(f"{type(item)} '{item!r}' is not a State, typing.Any, or typing.Self")
            rv.add(item)
    return {Any} if Any in rv else rv

_UNBOUND = "<unknown; State is not yet bound to a class>"

@attr.define(frozen=True, eq=False, repr=False, str=False, hash=False)
class State:
    """
    The state of a StateTracker. Declare States as class variables on a class which extends StateTracker, and the
    StateTracker will use them to model its internal state machine/graph.

    States will be named according to what field they're assigned to in their StateTracker class; see TODO PROPERTY NAME link.

    Parameters:
        initial (bool, default False): declare a state as the initial/default state that a StateTracker will start in.
            One and only one State in a StateTracker must be marked with "initial=True". If you need the initial state
            to be customizable e.g. at StateTracker construction time, set an unreachable state with "initial=True" and
            call transition() as appropriate in __init__. (TODO EXAMPLE)
        destinations (another State, typing.Any, typing.Self, or an iterable of any of those): states which the
            StateTracker can transition *to* from this State. If typing.Any is present, this state can transition to
            any other state in the StateTracker, including itself. If typing.Self is present, this state can be
            transitioned to itself (doing so will trigger a transition event (TODO link transition events)).
        sources (another State, typing.Any, typing.Self, or an iterable of any of those): states which the StateTracker
            can transition into this state *from*. Behaves the same as "destinations" otherwise.

    A transition is allowed if it is listed either in "sources" or "destinations"; it doesn't need to be listed in both.

    Redundant or duplicate entries in "sources" and "destinations" are ignored.

    Note that a State is only ever equal to itself. States with the same parameters from two different StateTrackers
    will not be treated as equivalent."""
    _destinations: Iterable[Transition] = attr.field(
        default=(),
        converter=_convert_state,
    )
    _sources: Iterable[Transition] = attr.field(
        default=(),
        converter=_convert_state,
    )
    initial: bool = attr.field(default=False)
    _name: str = attr.field(init=False, default=_UNBOUND)

    @property
    def name(self) -> str:
        """Returns the name to which the State was bound on its StateTracker.

        If a State is not yet bound to a StateTracker, returns the empty string. You *can* declare States apart from
        their StateTracker class, but probably *shouldn't*: a State can only be bound to one StateTracker (reusing
        it is an error at StateTracker class declaration time) and declaring them separately will likely lead to
        needlessly hard-to-follow code.
        """
        return self._name

    def __repr__(self):
        return f"<{self.__class__.__qualname__} {self.name}>"

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

    States must be declared explicitly at the class level. Supplying them via descriptors or __class_getitem__ or other
    metaprogramming techniques is not supported.

    Note: if using `attrs`, `dataclass` or other class-creation factories, all State objects declared at the class level
    should be annotated with ClassVar, otherwise subclass declaration may fail.

    Users can call StateTracker._transition() to move a StateTracker between states.

    Note that the "_transition()" method *is* a user-callable public API. It is declared as _private for convenience,
    since many uses of StateTracker may not want the state to be externally transitionable. You can declare a public
    transition method via code like:

    >>> class PubliclyTransitionableStateTracker(StateTracker):
    >>>      transition = StateTracker._transition
    >>>      ...
    """

    states: ClassVar[frozenset[State]]

    __slots__ = ('_state', '_condition')

    def __init__(self):
        self._state, = (s for s in self.states if s.initial)
        self._condition = threading.Condition()

    # We use a property to prevent accidental manual mutation
    # TODO property setter on transition
    @property
    def state(self) -> State:
        return self._state


    @classmethod
    def _structural_invariant(cls, cond: Any, msg: str):
        if not cond:
            raise TypeError(f"{cls.__qualname__}: {msg}")
        return cond

    @classmethod
    def _check_states(cls, seen: dict, source, *states):
        # TODO warn unreachable/prime DAG?
        for state in states:
            if state is not Any and state is not Self:
                cls._structural_invariant(
                    state in seen,
                    f'State "{source}" has a source or destination state that is not part of this state tracker: {state}'
                )

        if Any in states:
            yield from seen.keys()
        else:
            for state in states:
                if state is Self:
                    yield source
                else:
                    yield state

    def __init_subclass__(cls, abstract=False, **kwargs):
        super().__init_subclass__(**kwargs)
        is_abstract = abstract or inspect.isabstract(cls)
        destinations = defaultdict(set)
        initial = None
        members: dict[str, State] = dict(
            inspect.getmembers_static(cls, lambda i: isinstance(i, State))
        )
        seen_states: dict[State, str] = dict()
        for k, v in members.items():
            cls._structural_invariant(v not in seen_states, f"State {k} was already bound as {seen_states.get(v)}")
            seen_states[v] = k

        for name, state in members.items():
            if state.initial:
                cls._structural_invariant(initial is None, f"Multiple states with initial=True: '{state}' and '{initial}'")
                initial = state

            destinations[state].update(
                cls._check_states(seen_states, state, *state._destinations)
            )
            for source in cls._check_states(seen_states, state, *state._sources):
                destinations[source].add(state)
            cls._structural_invariant(
                state.name in  (_UNBOUND, name),  # Allow rebinding at the same name for subclassing, but not at different names.
                f"Cannot bind State {state!r}; it is already bound as '{name}'"
            )

        # TODO this invariant currently fails for WaitableStateTracker, even though https://github.com/python/cpython/issues/74008 is fixed
        # Why?!
        cls._structural_invariant(
            is_abstract or initial is not None,
            "No states were declared with initial=True; one and only one initial state is required"
        )
        for name, state in members.items():
            cls._structural_invariant(
                isinstance(name, str),
                "State name is not a string; you are probably doing something wrong and unsupported like constructing a class with invalid parameters using type(...)."
            )
            # Hack around frozen-ness to update the state object. This is safe, since it happens only at compile time
            # and only when a State object is first bound to its StateTracker. Attempts to rebind a State later fail
            # if its "name" is already set.
            object.__setattr__(state, "_name", name)
            object.__setattr__(state, "_destinations", destinations[state])
            # TODO wipe sources?
        cls.states = frozenset(members.values())

    def after_state_change(self, old_state: State, new_state: State, data: Any):
        """To be overridden by subclassers."""
        # NB: states may not match by the time this runs

    @final
    def _check_state(self, state: State) -> State:
        if state not in self.states:
            raise StateTrackerError(
                self,
            f"State '{state.name}' is not one of this class's states. Valid states:",
            *self.states,)
        return state

    @final
    def _transition(self, target: State, data: Any = None):
        with self._condition:
            old_state = self._state
            if target not in old_state._destinations:
                raise StateTrackerError(self, "Could not transition to state", self._check_state(target))
            self._state = target
            self._condition.notify_all()

        self.after_state_change(old_state, target, data)

    def require(self, *desired_states: State) -> Self:
        for s in desired_states:
            if self.state is self._check_state(s):
                return self
        raise StateTrackerError(self, "Not in required state(s)", *desired_states)


class WaitableStateTracker(StateTracker, ABC, abstract=True):
    type _FutureType = tuple[Self, State, State, Any]
    _waiters: dict[State, WeakSet[AwaitableFuture[_FutureType] | asyncio.Future[_FutureType]]]

    __slots__ = ('_waiters',)
    def __init__(self):
        super().__init__()
        self._waiters = {s: WeakSet() for s in self.states}

    @final
    def _wait_for_state(self, get_future, *states: State, **kwargs):
        if len(states) == 0:
            raise TypeError("At least one state must be supplied")
        future = get_future(**kwargs)
        with self._condition:
            for state in states:
                self._waiters[self._check_state(state)].add(future)
        return future

    def wait_for_state_from_event_loop(self, loop: asyncio.AbstractEventLoop, *states: State) -> asyncio.Future[_FutureType]:
        return self._wait_for_state(loop.create_future, *states)

    def wait_for_state(
        self, *states: State, **kwargs,
    ) -> AwaitableFuture[_FutureType]:
        # NB: the future returned here will be resolved once the StateTracker has passed through the given state, even
        # if it is no longer in that state. If a worker transitions twice in rapid succession, or a waiter is attached
        # and then not waited on until later, after the worker has passed through *states, it'll still resolve.
        # NB: cancelling the returned future does nothing.
        # NB: to get speed gains in async-only waiting code, pass future_class=asyncio.Future.
        # NB: if your done callbacks are slow, you'll break stuff (here and everywhere)
        return self._wait_for_state(AwaitableFuture, *states, **kwargs)

    @final  # NB: deliberate override of @final; this is the only place that is permitted
    def _transition(self, target: State, data: Any = None):

        with self._condition:
            old_state = self._state
            if target not in old_state._destinations:
                raise StateTrackerError(self, "Could not transition to state", self._check_state(target))
            old_waiters, self._state = self._waiters[target], target
            waiters = tuple(old_waiters)  # Not copy() because we don't need to hash -- TODO right? Or is copy faster even on weakset?
            old_waiters.clear()
            # Only wakes other things trying to _transition; the futures' internals will handle waking up whatever
            # routines are waiting on them.
            self._condition.notify_all()

        result = (self, old_state, target, data)
        for future in waiters:
            try:
                future.set_result(result)
            except (concurrent.futures.InvalidStateError, asyncio.InvalidStateError):
                pass

        self.after_state_change(old_state, target, data)
