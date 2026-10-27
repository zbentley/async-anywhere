from __future__ import annotations

import threading
from typing import Any, Iterable, Self

import attr

# NB: in this instance, we mean "the literal class object for the typing.Self type", not "the type of the invoking class".
type Transition = State | type[Any] | type[Self]


def _structural_invariant(cls: type, cond: Any, msg: str):
    if not cond:
        raise TypeError(f"{cls.__qualname__}: {msg}")
    return cond

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
# This lock orchestrates the extremely rare conflict risk of importing modules/declaring subclasses in parallel:
_binding_lock = threading.Lock()

def _binding(name: str, obj):
    # Identify a binding site for a variable. We can't use __qualname__ since some class builders (e.g. dataclasses)
    # don't reset it properly, so we use line number instead.
    return name, obj.__module__, obj.__name__, vars(obj).get("__firstlineno__")

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
    # As declared, until StateTracker.__init_subclass__ replaces it with every State this one can transition to
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
    _owner: type | None = attr.field(init=False, default=None)

    def __set_name__(self, owner: type, name: str):
        _structural_invariant(owner, isinstance(name, str), "State name is not a string; you are probably doing something wrong and unsupported like constructing a class with invalid parameters using type(...).")
        with _binding_lock:
            # Class builders that add slots (attrs, dataclasses) rebind States to a class rebuilt from a copy of its
            # namespace. This check makes sure that they're always bound at the same place.
            bound = self._owner
            _structural_invariant(
                owner,
                bound is None or _binding(name, owner) == _binding(self._name, bound),
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
