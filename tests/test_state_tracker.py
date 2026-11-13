import abc
import asyncio
import copy
import dataclasses
import gc
import os
import pickle
import threading
from typing import Any, ClassVar, Final, Self

import anyio
import attr
import pytest

from async_anywhere._util import ConditionProtected
from async_anywhere.exceptions import StateTrackerError
from async_anywhere.futures import AwaitableFuture
from async_anywhere.state_tracker import StateTracker, State
from async_anywhere.state_tracker.waitable import WaitableStateTracker
from tests.test_executors.utils import forked


class MyStateTracker(StateTracker):
    Initial: Final[State] = State(initial=True)
    Second: Final[State] = State(sources=[Initial])
    Third: Final[State] = State(sources=[Initial])

def test_basic_state_tracker():
    inst = MyStateTracker()
    assert inst.state is inst.Initial
    assert inst.state == inst.Initial

@pytest.mark.parametrize("thread_safe", [False, True], ids=["unlocked", "locked"])
def test_transition(thread_safe):
    inst = MyStateTracker(thread_safe=thread_safe)
    assert isinstance(inst._condition, ConditionProtected) is thread_safe
    inst._transition(MyStateTracker.Second)
    assert inst.state is MyStateTracker.Second
    with pytest.raises(StateTrackerError, match="Could not transition"):
        inst._transition(MyStateTracker.Third)
    assert inst.state is MyStateTracker.Second

def test_after_state_change_runs_under_the_lock_once_transitioned():
    seen = []

    class Hooked(StateTracker):
        Initial: Final[State] = State(initial=True)
        Next: Final[State] = State(sources=Initial)

        def _after_state_change(self, old_state, new_state, data):
            seen.append((self._condition._is_owned(), self.state, self.last_transition))

    Hooked()._transition(Hooked.Next, "data")
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
            A: Final[State] = State()

def test_abstract_tracker_may_declare_states():
    class Base(StateTracker, abc.ABC):
        A: Final[State] = State(initial=True)

    class Concrete(Base):
        pass

    assert Concrete().state is Base.A

def test_waitable_subclass():
    class Waitable(WaitableStateTracker):
        A: Final[State] = State(initial=True)

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
            NewInitial: Final[State] = State(initial=True)

def test_subclasses_cannot_shadow_states():
    with pytest.raises(TypeError, match="shadowed"):
        class Shadowed(MyStateTracker):
            Second = None

def test_states_cannot_be_rebound_added_or_deleted():
    class Child(MyStateTracker):
        pass

    for cls in (MyStateTracker, Child):
        with pytest.raises(TypeError, match="can't be rebound"):
            cls.Second = State()
        with pytest.raises(TypeError, match="can't be rebound"):
            cls.Second = None
        with pytest.raises(TypeError, match="can't be rebound"):
            cls.Fourth = State()
        with pytest.raises(TypeError, match="can't be removed"):
            del cls.Second
    assert vars(MyStateTracker)["Second"] in MyStateTracker.states and "Second" not in vars(Child)

    MyStateTracker.not_a_state = 1
    del MyStateTracker.not_a_state

def test_mixin_states_are_shared():
    class Mixin:
        Idle: Final[State] = State(initial=True)
        Busy: Final[State] = State(sources=[Idle], destinations=[Idle])

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
        Idle: Final[State] = State(initial=True, destinations=Any)
        Busy: Final[State] = State(destinations=Self)

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
        Idle: Final[State] = State(initial=True)

    with pytest.raises(TypeError, match="declared on one class"):
        class Both(Mixin, StateTracker):
            Other: Final[State] = State(sources=[Mixin.Idle])

def test_state_cannot_be_bound_twice():
    shared = State(initial=True)

    class First(StateTracker):
        A = shared

    with pytest.raises(TypeError, match="can only be bound once"):
        class Other(StateTracker):
            A = shared

    with pytest.raises(TypeError, match="can only be bound once"):
        class Aliased(StateTracker):
            A: Final[State] = State(initial=True)
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
        Start: ClassVar[Final[State]] = State(initial=True)
        End: ClassVar[Final[State]] = State(sources=[Start])

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
            Start: ClassVar[Final[State]] = State(initial=True)
            End: Final[State] = State(sources=[Start])  # A field, so the builder drops it from the class it rebuilds

def test_require():
    inst = MyStateTracker()
    assert inst.require(MyStateTracker.Initial) is inst
    assert inst.require(MyStateTracker.Second, MyStateTracker.Initial) is inst
    with pytest.raises(StateTrackerError, match="Not in required state"):
        inst.require(MyStateTracker.Second)

def test_try_transition():
    inst = MyStateTracker()
    assert inst._try_transition(MyStateTracker.Second, "data")
    assert inst.last_transition == (MyStateTracker.Initial, MyStateTracker.Second, "data")
    assert not inst._try_transition(MyStateTracker.Third) and inst.state is MyStateTracker.Second  # Refused
    with pytest.raises(TypeError, match="not one of this class's states"):
        inst._try_transition(MyWaitable.A)  # Not refused, but wrong

def test_error_is_copyable():
    with pytest.raises(StateTrackerError) as info:
        MyStateTracker()._transition(MyStateTracker.Initial)
    error = info.value
    error.add_note("a note")
    for duplicate in (copy.copy(error), pickle.loads(pickle.dumps(error)), error.copy()):
        assert type(duplicate) is StateTrackerError
        assert duplicate.args == error.args
        assert duplicate.__notes__ == ["a note"]


class MyWaitable(WaitableStateTracker):
    A: Final[State] = State(initial=True)
    B: Final[State] = State(sources=[A])
    C: Final[State] = State(sources=[A, B])

def test_wait_for_current_state_is_done():
    inst = MyWaitable()
    assert inst.wait_for_state(MyWaitable.A, MyWaitable.B, return_states=True).result(0) == (None, MyWaitable.A, None)
    waiting = inst.wait_for_state(MyWaitable.B, return_states=True)
    inst._transition(MyWaitable.B, "data")
    done = inst.wait_for_state(MyWaitable.B, return_states=True)
    assert done.result(0) == waiting.result(0) == (MyWaitable.A, MyWaitable.B, "data")
    inst._transition(MyWaitable.C)
    done = inst.wait_for_state(MyWaitable.C, MyWaitable.A, return_states=True)
    assert done.result(0) == (MyWaitable.B, MyWaitable.C, None)

def test_waiters_get_the_transitions_data_unless_asked_for_its_states_too():
    inst, data = MyWaitable(), object()
    assert inst.wait_for_state(MyWaitable.A).result(0) is None  # The initial state's, which no transition entered
    waiting, with_states = inst.wait_for_state(MyWaitable.B), inst.wait_for_state(MyWaitable.B, return_states=True)
    inst._transition(MyWaitable.B, data)
    assert waiting.result(0) is data and inst.wait_for_state(MyWaitable.B).result(0) is data  # Done already
    assert with_states.result(0) == (MyWaitable.A, MyWaitable.B, data)

def test_wait_for_state_validates_states():
    with pytest.raises(TypeError, match="not one of this class's states"):
        MyWaitable().wait_for_state(MyWaitable.A, MyStateTracker.Initial)

def test_wait_for_state_resolves_on_transition():
    inst = MyWaitable()
    future = inst.wait_for_state(MyWaitable.B, MyWaitable.C, return_states=True)
    assert not future.done()
    inst._transition(MyWaitable.B, "data")
    assert future.result(0) == (MyWaitable.A, MyWaitable.B, "data")
    inst._transition(MyWaitable.C)  # Its future, done, is still among C's waiters
    assert future.result(0)[1] is MyWaitable.B

class CyclicWaitable(WaitableStateTracker):
    A: Final[State] = State(initial=True, destinations=[Self])
    B: Final[State] = State(sources=[A])
    C: Final[State] = State(sources=[B], destinations=[A])

def test_ignore_current_state_waits_to_reenter_it():
    inst = CyclicWaitable()
    future = inst.wait_for_state(CyclicWaitable.A, ignore_current_state=True, return_states=True)
    assert not future.done()
    inst._transition(CyclicWaitable.A, "data")
    assert future.result(0) == (CyclicWaitable.A, CyclicWaitable.A, "data")

def test_ignore_current_state_waits_for_any_given_state():
    inst = CyclicWaitable()
    future = inst.wait_for_state(CyclicWaitable.A, CyclicWaitable.C, ignore_current_state=True, return_states=True)
    inst._transition(CyclicWaitable.B)
    assert not future.done()
    inst._transition(CyclicWaitable.C, "data")
    assert future.result(0) == (CyclicWaitable.B, CyclicWaitable.C, "data")

def test_waiters_refuse_cancellation():
    inst = MyWaitable()
    future = inst.wait_for_state(MyWaitable.B)
    assert not future.cancel() and not future.done()
    inst._transition(MyWaitable.B, "data")
    assert future.result(0) == "data"

def test_try_transition_resolves_waiters():
    inst = MyWaitable()
    future = inst.wait_for_state(MyWaitable.B)
    assert inst._try_transition(MyWaitable.B, "data") and future.result(0) == "data"
    assert not inst._try_transition(MyWaitable.B)  # B can't transition to itself

def test_dropped_waiters_are_not_resolved():
    inst = MyWaitable()
    resolved = []
    inst.wait_for_state(MyWaitable.B).add_done_callback(resolved.append)
    gc.collect()
    inst._transition(MyWaitable.B)
    assert len(resolved) == 0

async def test_prevent_blocking_wait_within():
    inst, here = MyWaitable(), (asyncio.get_running_loop(),)
    with pytest.raises(RuntimeError, match="would deadlock"):
        inst.wait_for_state(MyWaitable.B, prevent_blocking_wait_within=here).result(1)
    with pytest.raises(TimeoutError):  # Only the futures given it check
        inst.wait_for_state(MyWaitable.B).result(0)
    done = inst.wait_for_state(MyWaitable.A, prevent_blocking_wait_within=here)
    assert done.result(0) is None  # Done, so not checked

@pytest.mark.parametrize(
    "kwargs",
    [{"ignore_cancel": False}, {"transform_result": str}, {"nonsense": True}],
    ids=["ignore_cancel", "transform_result", "unknown"],
)
def test_wait_for_state_refuses_kwargs_its_futures_cannot_take(kwargs):
    with pytest.raises(TypeError):
        MyWaitable().wait_for_state(MyWaitable.B, **kwargs)

@pytest.mark.backends("asyncio", "uvloop", "trio")
async def test_wait_for_state_across_threads():
    inst = MyWaitable()
    future = inst.wait_for_state(MyWaitable.B)
    thread = threading.Thread(target=inst._transition, args=(MyWaitable.B, "data"))
    thread.start()
    with anyio.fail_after(5):
        assert await future == "data"
    thread.join()

@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork()")
def test_a_forked_process_resets_trackers_locks_before_futures_callbacks_run():
    inst, holding, release = MyStateTracker(), threading.Event(), threading.Event()
    # Cancelled by the futures' fork handler, whose callback transitions the tracker: so its lock must be reset by then
    cancelled = AwaitableFuture(cancel_pending_in_forked_child=True)
    cancelled.add_done_callback(lambda _: inst._transition(MyStateTracker.Second), run_in_forked_child=True)

    def hold():
        with inst._condition:  # As a transition would, as the process forks
            holding.set()
            release.wait(10)

    holder = threading.Thread(target=hold)
    holder.start()
    holding.wait(2)
    with forked() as f:
        if f.child:
            assert cancelled.cancelled() and inst.state is MyStateTracker.Second
        else:
            release.set()
    holder.join()
    assert inst.state is MyStateTracker.Initial and not cancelled.done()
