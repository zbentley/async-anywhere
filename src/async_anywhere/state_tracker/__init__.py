from __future__ import annotations

import inspect
import threading
from abc import ABC
from collections import defaultdict
from threading import RLock
from typing import Any, ClassVar, Collection, ContextManager, Iterable, Self, TypeAlias, Union

import attr

from async_anywhere.exceptions import StateTrackerError

# NB: in this instance, we mean "the literal class object for the typing.Self type", not "the type of the invoking class".
Transition: TypeAlias = Union["State", type[Any], type[Self]]


def _structural_invariant(cls: type, cond: Any, msg: str):
    if not cond:
        raise TypeError(f"{cls.__qualname__}: {msg}")
    return cond

_UNBOUND = "<unknown; State is not yet bound to a class>"
# This lock orchestrates the extremely rare conflict risk of importing modules/declaring subclasses in parallel:
_binding_lock = threading.Lock()

@attr.define(frozen=True, eq=False, repr=False, str=False, hash=False, init=False)
class State:
    """
    The state of a StateTracker. Declare States as class variables on a class which extends StateTracker, and the
    StateTracker will use them to model its internal state machine/graph.

    States will be named according to what field they're assigned to in their StateTracker class; see TODO PROPERTY NAME link.

    Note:
        A State is only ever equal to itself. States with the same parameters from two different StateTrackers
        will not be treated as equivalent.
    """
    # As declared, until StateTracker.__init_subclass__ replaces it with every State this one can transition to
    _destinations: Collection[Transition] = attr.field()
    _sources: Transition | Collection[Transition] = attr.field()
    initial: bool = attr.field()
    """Some ogher stuff"""
    terminal: bool = attr.field(default=False, init=False)
    """Some stuff!"""
    _name: str = attr.field(init=False, default=_UNBOUND)
    _owner: type | None = attr.field(init=False, default=None)

    def __init__(
        self,
        *,
        initial: bool = False,
        destinations: Transition | Collection[Transition] = (),
        sources: Transition | Collection[Transition] = (),
    ):
        """
        Args:
            initial: declare a state as the initial/default state that a StateTracker will start in.
                One and only one State in a StateTracker must be marked with "initial=True". If you need the initial state
                to be customizable e.g. at StateTracker construction time, set an unreachable state with "initial=True" and
                call transition() as appropriate in __init__. (TODO EXAMPLE)
            destinations (another State, typing.Any, typing.Self, or an iterable of any of those): states which the
                StateTracker can transition *to* from this State. If typing.Any is present, this state can transition to
                any other state in the StateTracker, including itself. If typing.Self is present, this state can be
                transitioned to itself (doing so will trigger a transition event (TODO link transition events)).

                A transition is allowed if it is listed either in "sources" or "destinations"; it doesn't need to be listed in both.

                Redundant or duplicate entries in "sources" and "destinations" are ignored.

            sources: states which the StateTracker
                can transition into this state *from*. Behaves the same as "destinations" otherwise.
        """
        self.__attrs_init__(
            initial=initial,
            destinations=self._convert_state(destinations),
            sources=self._convert_state(sources),
        )

    @staticmethod
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

    @staticmethod
    def _binding(name: str, obj):
        # Identify a binding site for a variable. We can't use __qualname__ since some class builders (e.g. dataclasses)
        # don't reset it properly, so we use line number instead.
        return name, obj.__module__, obj.__name__, vars(obj).get("__firstlineno__")

    def __set_name__(self, owner: type, name: str):
        _structural_invariant(owner, isinstance(name, str), "State name is not a string; you are probably doing something wrong and unsupported like constructing a class with invalid parameters using type(...).")
        with _binding_lock:
            # Class builders that add slots (attrs, dataclasses) rebind States to a class rebuilt from a copy of its
            # namespace. This check makes sure that they're always bound at the same place.
            bound = self._owner
            _structural_invariant(
                owner,
                bound is None or self._binding(name, owner) == self._binding(self._name, bound),
                f"Cannot bind State {bound.__qualname__ if bound else ''}.{self._name} as '{name}'; a State can only be bound once"
            )
            # Hack around frozen-ness to update the state object. This is safe, since it happens only at compile time
            # and only when a State object is first bound to its StateTracker. Attempts to rebind a State later fail
            # if its "name" is already set.
            object.__setattr__(self, "_owner", owner)
            object.__setattr__(self, "_name", name)

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

    Example:

        >>> class MyStateTracker(StateTracker):
        >>>    InitialState = State(initial=True)
        >>>    OtherState = State(sources=[InitialState])
        >>>    ...

    When this is done, instances of MyStateTracker will start with ``inst.state == MyStateTracker.InitialState``.

    States must be declared together on one class; they cannot be combined or layered across subclasses. States must be
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

        Args:
            lock: context manager representing the lock that will be held by each transition, and its
                _after_state_change(). The default is a fresh RLock, so _after_state_change() can use the StateTracker
                again. Pass contextlib.nullcontext() to take no lock, and only use the StateTracker from one thread at
                a time.
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

    def _after_state_change(self, old_state: State, new_state: State, data: Any):
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
            self._after_state_change(old_state, target, data)

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

