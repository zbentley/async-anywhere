import abc
import asyncio
import contextlib
import copy
import dataclasses
import gc
import pickle
import threading
from typing import Any, ClassVar, Self

import anyio
import attr
import pytest

from async_anywhere._util import copy_exception
from async_anywhere.state_tracker import StateTracker, State, StateTrackerError
from async_anywhere.state_tracker.waitable import WaitableStateTracker


class MyStateTracker(StateTracker):
    Initial = State(initial=True)
    Second = State(sources=[Initial])
    Third = State(sources=[Initial])

def test_basic_state_tracker():
    inst = MyStateTracker()
    assert inst.state is inst.Initial
    assert inst.state == inst.Initial

@pytest.mark.parametrize("lock", [contextlib.nullcontext(), threading.Lock()], ids=["unlocked", "locked"])
def test_transition(lock):
    inst = MyStateTracker(lock=lock)
    inst._transition(MyStateTracker.Second)
    assert inst.state is MyStateTracker.Second
    with pytest.raises(StateTrackerError, match="Could not transition"):
        inst._transition(MyStateTracker.Third)
    assert inst.state is MyStateTracker.Second

def test_after_state_change_runs_under_the_lock_once_transitioned():
    seen = []

    class Hooked(StateTracker):
        Initial = State(initial=True)
        Next = State(sources=Initial)

        def after_state_change(self, old_state, new_state, data):
            seen.append((self._condition._is_owned(), self.state, self._last_transition))

    Hooked(lock=threading.RLock())._transition(Hooked.Next, "data")
    assert seen == [(True, Hooked.Next, (Hooked.Initial, Hooked.Next, "data"))]

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
    with pytest.raises(TypeError, match="not one of this class's states"):
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

def test_any_and_self_survive_resolving_again():
    class Mixin:
        Idle = State(initial=True, destinations=Any)
        Busy = State(destinations=Self)

    class A(Mixin, StateTracker):
        pass

    class B(Mixin, StateTracker):  # Resolves Mixin's already resolved destinations again
        pass

    for inst in (A(), B()):
        inst._transition(Mixin.Idle)
        inst._transition(Mixin.Busy)
        inst._transition(Mixin.Busy)
        with pytest.raises(StateTrackerError):
            inst._transition(Mixin.Idle)

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

@pytest.mark.parametrize(
    "builder", [attr.define(init=False), dataclasses.dataclass(slots=True, init=False)], ids=["attrs", "dataclass"]
)
def test_slotted_class_builders_reject_non_classvar_states(builder):
    with pytest.raises(TypeError, match="not part of this state tracker"):
        @builder
        class Declared(StateTracker):
            Start: ClassVar[State] = State(initial=True)
            End: State = State(sources=[Start])  # A field, so the builder drops it from the class it rebuilds

def test_require():
    inst = MyStateTracker()
    assert inst.require(MyStateTracker.Initial) is inst
    assert inst.require(MyStateTracker.Second, MyStateTracker.Initial) is inst
    with pytest.raises(StateTrackerError, match="Not in required state"):
        inst.require(MyStateTracker.Second)

def test_try_transition():
    inst = MyStateTracker()
    assert inst._try_transition(MyStateTracker.Second, "data")
    assert inst._last_transition == (MyStateTracker.Initial, MyStateTracker.Second, "data")
    assert not inst._try_transition(MyStateTracker.Third) and inst.state is MyStateTracker.Second  # Refused
    with pytest.raises(TypeError, match="not one of this class's states"):
        inst._try_transition(MyWaitable.A)  # Not refused, but wrong

def test_error_is_copyable():
    with pytest.raises(StateTrackerError) as info:
        MyStateTracker()._transition(MyStateTracker.Initial)
    error = info.value
    error.add_note("a note")
    for duplicate in (copy.copy(error), pickle.loads(pickle.dumps(error)), copy_exception(error)):
        assert type(duplicate) is StateTrackerError
        assert duplicate.args == error.args
        assert duplicate.__notes__ == ["a note"]


class MyWaitable(WaitableStateTracker):
    A = State(initial=True)
    B = State(sources=[A])
    C = State(sources=[A, B])

def test_wait_for_current_state_is_done():
    inst = MyWaitable()
    assert inst.wait_for_state(MyWaitable.A, MyWaitable.B).result(0) == (None, MyWaitable.A, None)
    waiting = inst.wait_for_state(MyWaitable.B)
    inst._transition(MyWaitable.B, "data")
    assert inst.wait_for_state(MyWaitable.B).result(0) == waiting.result(0) == (MyWaitable.A, MyWaitable.B, "data")
    inst._transition(MyWaitable.C)
    assert inst.wait_for_state(MyWaitable.C, MyWaitable.A).result(0) == (MyWaitable.B, MyWaitable.C, None)

def test_wait_for_state_validates_states():
    with pytest.raises(TypeError, match="not one of this class's states"):
        MyWaitable().wait_for_state(MyWaitable.A, MyStateTracker.Initial)

def test_wait_for_state_resolves_on_transition():
    inst = MyWaitable()
    future = inst.wait_for_state(MyWaitable.B, MyWaitable.C)
    assert not future.done()
    inst._transition(MyWaitable.B, "data")
    assert future.result(0) == (MyWaitable.A, MyWaitable.B, "data")
    inst._transition(MyWaitable.C)  # Its future, done, is still among C's waiters
    assert future.result(0)[1] is MyWaitable.B

def test_waiters_refuse_cancellation():
    inst = MyWaitable()
    future = inst.wait_for_state(MyWaitable.B)
    assert not future.cancel() and not future.done()
    inst._transition(MyWaitable.B, "data")
    assert future.result(0) == (MyWaitable.A, MyWaitable.B, "data")

def test_try_transition_resolves_waiters():
    inst = MyWaitable()
    future = inst.wait_for_state(MyWaitable.B)
    assert inst._try_transition(MyWaitable.B, "data") and future.result(0) == (MyWaitable.A, MyWaitable.B, "data")
    assert not inst._try_transition(MyWaitable.B)  # B can't transition to itself

def test_dropped_waiters_are_not_resolved():
    inst = MyWaitable()
    resolved = []
    inst.wait_for_state(MyWaitable.B).add_done_callback(resolved.append)
    gc.collect()
    inst._transition(MyWaitable.B)
    assert len(resolved) == 0

def test_prevent_blocking_wait_in_threads():
    # A one-shot iterator, which must still cover every future the tracker makes
    inst = MyWaitable(prevent_blocking_wait_in_threads=iter([threading.current_thread()]))
    for _ in range(2):
        with pytest.raises(RuntimeError, match="would deadlock"):
            inst.wait_for_state(MyWaitable.B).result(0)
    assert inst.wait_for_state(MyWaitable.A).result(0) == (None, MyWaitable.A, None)  # Done, so not checked

async def test_prevent_blocking_wait_in_loops():
    inst = MyWaitable(prevent_blocking_wait_in_loops=asyncio.get_running_loop())
    with pytest.raises(RuntimeError, match="would deadlock"):
        inst.wait_for_state(MyWaitable.B).result(0)

@pytest.mark.backends("asyncio", "uvloop", "trio")
async def test_wait_for_state_across_threads():
    inst = MyWaitable()
    future = inst.wait_for_state(MyWaitable.B)
    thread = threading.Thread(target=inst._transition, args=(MyWaitable.B, "data"))
    thread.start()
    with anyio.fail_after(5):
        assert await future == (MyWaitable.A, MyWaitable.B, "data")
    thread.join()
