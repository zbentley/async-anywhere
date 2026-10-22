import abc
import dataclasses
from typing import ClassVar

import attr
import pytest

from async_anywhere.state_tracker import StateTracker, State, StateTrackerError, WaitableStateTracker


class MyStateTracker(StateTracker):
    Initial = State(initial=True)
    Second = State(sources=[Initial])
    Third = State(sources=[Initial])

def test_basic_state_tracker():
    inst = MyStateTracker()
    assert inst.state is inst.Initial
    assert inst.state == inst.Initial

def test_concrete_tracker_requires_states():
    with pytest.raises(TypeError, match="initial=True"):
        class Empty(StateTracker):
            pass

def test_abstract_tracker_may_omit_states():
    class ViaABC(StateTracker, abc.ABC):
        pass

    class ViaAbstractMethod(StateTracker):
        @abc.abstractmethod
        def f(self): ...

    assert ViaABC.states == ViaAbstractMethod.states == frozenset()

def test_abstractness_is_not_inherited():
    class Base(StateTracker, abc.ABC):
        pass

    with pytest.raises(TypeError, match="initial=True"):
        class Concrete(Base):
            pass

def test_abstract_tracker_with_states_requires_initial():
    with pytest.raises(TypeError, match="initial=True"):
        class Partial(StateTracker, abc.ABC):
            A = State()

def test_abstract_tracker_may_declare_states():
    class Base(StateTracker, abc.ABC):
        A = State(initial=True)

    class Concrete(Base):
        pass

    assert Concrete().state is Base.A

def test_waitable_subclass():
    class Waitable(WaitableStateTracker):
        A = State(initial=True)

    assert Waitable().state is Waitable.A

def test_subclasses_inherit_states_but_cannot_add_them():
    class Child(MyStateTracker):
        pass

    assert Child.states == MyStateTracker.states
    assert Child().state is MyStateTracker.Initial

    fourth = State(sources=[MyStateTracker.Second])
    with pytest.raises(TypeError, match="declared on one class"):
        class Extended(MyStateTracker):
            Fourth = fourth

    inst = MyStateTracker()
    inst._transition(MyStateTracker.Second)
    with pytest.raises(StateTrackerError):
        inst._transition(fourth)

def test_subclasses_cannot_declare_another_initial_state():
    with pytest.raises(TypeError, match="declared on one class"):
        class Overridden(MyStateTracker):
            NewInitial = State(initial=True)

def test_subclasses_cannot_shadow_states():
    with pytest.raises(TypeError, match="shadowed"):
        class Shadowed(MyStateTracker):
            Second = None

def test_mixin_states_are_shared():
    class Mixin:
        Idle = State(initial=True)
        Busy = State(sources=[Idle], destinations=[Idle])

    class A(Mixin, StateTracker):
        pass

    class B(Mixin, StateTracker):
        pass

    a, b = A(), B()
    assert a.state is b.state is Mixin.Idle
    a._transition(Mixin.Busy)
    b._transition(Mixin.Busy)
    assert a.state is b.state is Mixin.Busy

def test_mixin_states_cannot_be_added_to():
    class Mixin:
        Idle = State(initial=True)

    with pytest.raises(TypeError, match="declared on one class"):
        class Both(Mixin, StateTracker):
            Other = State(sources=[Mixin.Idle])

def test_state_cannot_be_bound_twice():
    shared = State(initial=True)

    class First(StateTracker):
        A = shared

    with pytest.raises(TypeError, match="can only be bound once"):
        class Other(StateTracker):
            A = shared

    with pytest.raises(TypeError, match="can only be bound once"):
        class Aliased(StateTracker):
            A = State(initial=True)
            B = A

@pytest.mark.parametrize(
    "builder", [attr.define(init=False), dataclasses.dataclass(slots=True, init=False)], ids=["attrs", "dataclass"]
)
def test_slotted_class_builders(builder):
    @builder
    class AbstractBase(StateTracker, abc.ABC):
        pass

    @builder
    class AbstractMethodBase(StateTracker):
        @abc.abstractmethod
        def f(self): ...

    @builder
    class Declared(AbstractBase):
        Start: ClassVar[State] = State(initial=True)
        End: ClassVar[State] = State(sources=[Start])

    assert "__slots__" in vars(Declared)
    assert Declared.Start._owner is Declared
    inst = Declared()
    inst._transition(Declared.End)
    assert inst.state is Declared.End
