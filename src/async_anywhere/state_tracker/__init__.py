from __future__ import annotations

import inspect
import threading
from abc import ABC, ABCMeta
from collections import defaultdict
from contextlib import nullcontext
from typing import Any, ClassVar, Collection, Iterable, Self, TypeAlias, Union, final, Literal

import attr

from async_anywhere._util import ConditionProtected
from async_anywhere.exceptions import StateTrackerError

# NB: in this instance, we mean "the literal class object for the typing.Self type", not "the type of the invocant".
Transition: TypeAlias = Union["State", type[Any], type[Self]]


def _structural_invariant(cls: type, cond: Any, msg: str):
    if not cond:
        raise TypeError(f"{cls.__qualname__}: {msg}")
    return cond

# This lock orchestrates the extremely rare conflict risk of importing modules/declaring subclasses in parallel:
_binding_lock = threading.Lock()
_UNLOCKED = nullcontext()  # Every thread-unsafe StateTracker's lock: it holds nothing, so one is enough

@attr.define(frozen=True, eq=False, repr=False, str=False, hash=False, init=False)
class State:
    r"""
    The state of a StateTracker. Declare States as class variables on a class which extends StateTracker, and the
    StateTracker will use them to model its internal state machine/graph.

    Annotate States as :py:obj:`~typing.Final` on ordinary subclasses, and ``ClassVar[Final[State]]`` on subclasses
    that use ``attrs``, ``dataclass`` or other class-creation factories.

    Warning:
        Setting ``State``\ s on a ``StateTracker`` at any point after class declaration time (e.g.
        ``MyTracker.Foo = State()``) is not allowed.

    Example:
        >>> class MyTracker(StateTracker):
        >>>     First: Final[State] = State(initial=True)
        >>>
        >>> @attrs.define
        >>> class MyAttrsTracker(StateTracker):
        >>>     First: ClassVar[Final[State]] = State(initial=True)

    Note:
        A State is only ever equal to itself. States with the same parameters from two different StateTrackers
        will not be treated as equivalent.
    """
    # As declared, until StateTracker.__init_subclass__ replaces it with every State this one can transition to
    _destinations: Collection[Transition] = attr.field()
    _sources: Collection[Transition] = attr.field()
    initial: bool = attr.field()
    terminal: bool = attr.field(default=False, init=False)
    """Whether this State can transition to any other States. Read-only.
    
    Example:
        >>> class MyTracker(StateTracker):
        >>>     Foo: Final[State] = State()
        >>>     First: Final[State] = State(initial=True, destinations=[Foo])
        >>>     Bar: Final[State] = State(sources=[First])
        >>>
        >>> assert not MyTracker.First.terminal
        >>> assert MyTracker.Bar.terminal
        >>> assert MyTracker.Foo.terminal
    
    If a State is not yet bound to a StateTracker, the behavior of this field is undefined.
    """
    name: str = attr.field(init=False, default="")
    """The name to which the State was bound on its StateTracker class. Read-only.
    
    Example:
    
        >>> class Foo(StateTracker):
        >>>     MyFirstState: Final[State] = State(initial=True)
        >>>
        >>> assert Foo.MyFirstState.name == "MyFirstState"

    If a State is not yet bound to a StateTracker, this is the empty string. You *can* declare States apart from
    their StateTracker class, but probably *shouldn't*: a State can only be bound to one StateTracker (reusing
    it is an error at StateTracker class declaration time) and declaring them separately will likely lead to
    needlessly hard-to-follow code.
    """
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
            initial: Mark this state as the initial/default state that a :py:class:`StateTracker` will start in. One
                and only one State in a StateTracker must be marked with ``initial=True``.

                If you need the initial state to be customizable e.g. at ``StateTracker`` construction time, set a
                private/unreachable state with ``initial=True`` and call :py:func:`_transition` as appropriate at
                initialization time.

            destinations: States which the StateTracker can transition *to* from this State. Accepts another
                :py:class:`State` declared in the same class as this one, :py:obj:`typing.Any`, :py:obj:`typing.Self`,
                or an iterable of any of those.

                If ``typing.Any`` is present, this state can transition to any other state in the ``StateTracker``,
                including itself.

                If ``typing.Self`` is present, this state can be transitioned to itself (doing so will trigger a
                transition event and will wake up any :py:func:`WaitableStateTracker.wait_for_state` calls subscribed
                to that transition).

                A transition is allowed if it is listed either in ``sources`` or ``destinations``; it doesn't need to be
                listed in both.

                Redundant or duplicate entries in "sources" and "destinations" are ignored.

                Note:
                    The references to :py:obj:`typing.Any` and :py:obj:`typing.Self` here are *literal* references. That
                    is: you can pass those specific values to this class as sentinels, like this:

                    >>> import typing
                    >>> Initial = State(initial=True, destinations=typing.Any)
                    >>> Second = State(sources=[typing.Self, Initial])

            sources: States which the ``StateTracker`` can transition into this state *from*. Behaves the same as
                ``destinations`` otherwise.

                A transition is allowed if it is listed either in ``sources`` or ``destinations``; it doesn't need to be
                listed in both.
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
                bound is None or self._binding(name, owner) == self._binding(self.name, bound),
                f"Cannot bind State {bound.__qualname__ if bound else ''}.{self.name} as '{name}'; a State can only be bound once"
            )
            # Hack around frozen-ness to update the state object. This is safe, since it happens only at compile time
            # and only when a State object is first bound to its StateTracker. Attempts to rebind a State later fail
            # if its "name" is already set.
            object.__setattr__(self, "_owner", owner)
            object.__setattr__(self, "name", name)

    def __repr__(self):
        name = self.name or "<unknown; State is not yet bound to a class>"
        return f"<{self.__class__.__qualname__} {name}>"


class _StateTrackerMeta(ABCMeta):
    # __set_name__ only runs when a class is created, so assignments to the class afterwards are caught here
    def __setattr__(cls, name: str, value: Any):
        _structural_invariant(
            cls,
            not isinstance(value, State) and not isinstance(inspect.getattr_static(cls, name, None), State),
            f"Cannot set {name}; States can't be rebound, or added to a class after it's declared"
        )
        super().__setattr__(name, value)

    def __delattr__(cls, name: str):
        _structural_invariant(
            cls,
            not isinstance(inspect.getattr_static(cls, name, None), State),
            f"Cannot delete {name}; States can't be removed from a class after it's declared"
        )
        super().__delattr__(name)


class StateTracker(ABC, metaclass=_StateTrackerMeta):
    r"""
    An abstract class which contains one or more :py:class:`State`\ s as class variables, and can move between them in
    a thread-safe way. Instances have a field :py:attr:`state` which contains the current ``State`` of the instance,
    and have several other methods to inspect and assert on states and transitions between them.

    Users can call :py:func:`_transition` or :py:func:`_try_transition` to move a ``StateTracker`` between states.

    Users are intended to subclass StateTracker for their own use-cases. When subclassing StateTracker, users must
    declare one or more State objects as class-level fields.

    Warning:
        Do not declare invalid (unreachable) States on a StateTracker subclass. At the moment, no topological validation
        of the state graph is performed, but validation (and resulting validation errors) may be added in a future
        release.

    Example:

        >>> class MyStateTracker(StateTracker):
        >>>    InitialState: Final[State] = State(initial=True)
        >>>    OtherState: Final[State] = State(sources=[InitialState])
        >>>
        >>>    def foo(self):
        >>>         self._transition(self.OtherState, "some data")
        >>>
        >>> t = MyStateTracker()
        >>> assert t.state is MyStateTracker.InitialState is t.InitialState
        >>> t.foo()
        >>> assert t.state is MyStateTracker.OtherState is t.OtherState
        >>> t.require(t.OtherState)
        >>>
        >>> # Raises StateTrackerError (an InvalidStateError): not in required state
        >>> t.require(t.InitialState)
        >>> # Raises StateTrackerError: cannot transition from OtherState -> OtherState.
        >>> t.foo()

    .. admonition:: Subclassing

        All of a StateTracker's States must be declared together on one class; they cannot be combined or layered across
        subclasses. States may be declared in one of the following locations:

        1. On a concrete StateTracker subclass, as explicit, literal class fields (not via descriptors or ``__class_getitem__``).
        2. On any class that is inherited by a StateTracker subclass (as explicit, literal class fields). This allows for
           common state graphs to be used by multiple different StateTracker subclasses via a mixin.

        All concrete StateTracker subclasses must declare States. Abstract StateTracker subclasses must be extended by a
        concrete class which declares States.

        StateTracker's metaclass derives from :py:class:`abc.ABCMeta`, so to also inherit from a class with another
        ``ABCMeta``-derived metaclass (e.g. a :py:class:`typing.Protocol`), declare a metaclass combining both, like
        ``class Meta(type(StateTracker), type(MyProtocol)): ...``, and pass it as ``metaclass=Meta``.

        If subclasses use ``attrs``, ``dataclass`` or other class-creation factories, all State objects declared at the class
        level must be annotated with ``ClassVar[Final[State]]``, otherwise subclass declaration may fail.

    Warning:
        Setting ``State``\ s on a ``StateTracker`` at any point after class declaration time (e.g.
        ``MyTracker.Foo = State()``) is not allowed.
    """

    states: ClassVar[frozenset[State]]
    _abstract: ClassVar[bool]
    _initial_transition: ClassVar[tuple[None, State | None, None]]

    # (old state, new state, data) of the last transition, or (None, initial state, None) before any: one tuple, so that
    # lock-free readers always see a state and its data together
    _last_transition: tuple[State | None, State, object]

    __slots__ = ('_last_transition', '_condition')

    def __init__(self, thread_safe: bool = True) -> None:
        r"""
        Args:
            thread_safe: If True, a per-instance thread-recursive lock is held by :py:func:`_transition`,
                :py:func:`_try_transition`, and :py:func:`_after_state_change`. All other methods are lock-free and
                thread-safe to call regardless of the value of this argument.

                If False, no locks are used (which means that a ``StateTracker`` can only be safely used from one thread
                at a time).

                Regardless of the value of ``thread_safe``, ``StateTracker``\ s are safe to use in the presence of
                :py:func:`os.fork`, though some care is required. See :ref:`state_tracker_fork_safety`.
        """
        self._last_transition = self._initial_transition
        self._condition = ConditionProtected(None) if thread_safe else _UNLOCKED

    @final
    @property
    def state(self) -> State:
        """The current :py:class:`State` of the invocant StateTracker."""
        return self._last_transition[1]

    @final
    @property
    def last_transition(self) -> tuple[State | None, State, object]:
        """Information about the last state transition (if any) performed on this StateTracker.

        A 3-tuple containing:

        1. The :py:class:`State` that the StateTracker was in before its most recent transition.
        2. The :py:class:`State` that this StateTracker was transitioned *into*, as returned by :py:attr:`state`.
        3. The data payload passed to :py:func:`_transition` during the last successful transition event, or None if no
           data was passed or no transition has yet been performed.

        Note:
            The value provided by this property is retrieved in an atomic, thread-safe manner, but simultaneous calls to
            ``_transition`` or ``_try_transition`` from other threads may cause repeated reads of this property to
            observe multiple values, and it may disagree with the :py:attr:`state` property.
        """
        return self._last_transition

    @final
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

    def _after_state_change(self, old_state: State, new_state: State, data: object):
        """
        Called immediately after the invocant successfully transitions into a new State. *Not* called when the invocant
        is constructed in its initial state.

        Does nothing by default; can optionally be overridden by subclasses (``super()`` does not need to be called by
        overriders).

        This method is called by :py:func:`_transition` while the lock (see the ``thread_safe`` constructor parameter)
        is held, and will thus never be called concurrently on the same StateTracker instance. If you instead want to
        react to state changes without the lock being held, override ``_transition`` in a subclass instead.

        Args:
            old_state: the :py:class:`State` that the invocant was in before its most recent transition.
            new_state: the :py:class:`State` that the invocant was transitioned *into*. Depending on concurrency and the
                ``thread_safe`` constructor parameter, this may or may not be the same value as the
                :py:attr:`state` attribute.
            data: the value that was supplied to :py:func:`_transition` when transitioning from ``old_state`` to
                ``new_state``, or None if a value was not supplied.
        """

    @final
    def _check_state(self, state: State) -> State:
        if state not in self.states:
            err = StateTrackerError(
                self,
            f"State '{state.name}' is not one of this class's states. Valid states:",
            *self.states)
            raise TypeError(err.args[0])
        return state

    def _transition(self, target: State, data: object = None) -> State:
        r"""
        Move the invocant ``StateTracker`` from its current State (stored in :py:attr:`state`) to the specified
        ``target`` State, storing an optional data payload related to the transition.

        If this method is overridden in a subclass, ``super()._transition`` must be called.

        Note:
             Despite its name starting with an underscore, :py:meth:`_transition` *is* a user-callable public API. It
             is declared as _private for convenience, since many uses of StateTracker may not want the state to be
             externally transitionable. You can declare a public ``transition()`` method via code like:

            >>> class PubliclyTransitionableStateTracker(StateTracker):
            >>>      transition = StateTracker._transition  # or WaitableStateTracker._transition
            >>>      ...

        Args:
            target: the State that the invocant should be transitioned into.
            data: a payload to attach to the transition event. Will be provided to :py:func:`_after_state_change` and
                returned by :py:attr:`last_transition`. ``data`` is stored on the invocant (and thus not garbage
                collected) until the next successful call to ``_transition``.

        Raises:
            :py:class:`StateTrackerError`: if the current :py:attr:`state` cannot be transitioned to ``target`` (via
                the ``sources`` and ``destinations`` constructor parameters to :py:class:`State`).

            :py:class:`TypeError`: if ``target`` is not one of the invocant's declared :py:class:`State`\ s.

        Returns:
            The prior state of the invocant before it was transitioned to ``target``.
        """
        with self._condition:
            old_state = self._last_transition[1]
            if target not in old_state._destinations:
                raise StateTrackerError(self, "Could not transition to state", self._check_state(target))
            self._last_transition = (old_state, target, data)
            # TODO fork unsafety is between these lines
            self._after_state_change(old_state, target, data)
        return old_state

    @final
    def _try_transition(self, target: State, data: object = None) -> State | Literal[False]:
        r"""
        Tries to call :py:func:`_transition` and returns False if it raises :py:class:`StateTrackerError`.

        Args:
            target: see :py:func:`_transition`.
            data: see :py:func:`_transition`.

        Raises:
            :py:class:`TypeError`: if ``target`` is not one of the invocant's declared :py:class:`State`\ s.

        Returns:
            The prior state of the invocant before it was transitioned to ``target``, if the transition was successful,
            or False, if ``target`` was a valid State for the invocant, but there was no source/destination path from
            the invocant's current :py:attr:`state` to ``target``.

            Since State objects are always truthy, the return value of this method can be safely tested as a boolean,
            e.g. ``if old := tracker._try_transition(tracker.NewState): ...``.
        """
        try:
            return self._transition(target, data)
        except StateTrackerError:
            return False

    @final
    def require(self, desired_state: State, *desired_states: State) -> Self:
        r"""
        Asserts that the invocant ``StateTracker`` is in one of several specified :py:class:`State`\ s.

        Args:
            desired_state: a ``State`` to require.
            *desired_states: additional ``State``\ s to require. ``require()`` is satisfied if :py:attr:`state`
                is *any* of the supplied ``desired_state`` or ``desired_states`` values.

        Raises:
            :py:class:`StateTrackerError`: if the current :py:attr:`state` is not ``desired_state`` or any of ``desired_states``.

            :py:class:`TypeError`: if any of ``desired_state`` or ``desired_states`` is not one of the invocant's
                declared :py:class:`State`\ s.

        Returns:
            The invocant StateTracker object, for convenience in chaining.
        """
        current_state = self.state
        if current_state is desired_state or current_state in desired_states:
            return self
        # Validate the inputs on the failure path so the happy path stays fast:
        for s in (desired_state, *desired_states):
            self._check_state(s)
        raise StateTrackerError(self, "Not in required state(s)", desired_state, *desired_states)

