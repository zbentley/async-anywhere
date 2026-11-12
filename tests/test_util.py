"""Tests for async_anywhere._util's helpers."""
from __future__ import annotations

import concurrent.futures
import gc
import os
import sys
import threading
import time
import weakref

import pytest
from attrs import define, field

from async_anywhere._util import AnyEventLoop, BlockingChecker, ConditionProtected, validate_coroutine_function
from async_anywhere.exceptions import WouldBlock
from tests.test_executors.utils import forked


def interrupting(method):
    """Wraps method, raising KeyboardInterrupt (with what it returned) as it returns, as a signal handler can."""

    def interrupted(*args):
        raise KeyboardInterrupt(method(*args))

    return interrupted


def test_entering_lets_go_of_a_lock_taken_as_a_signal_handler_raises():
    protected = ConditionProtected(None)
    lock, protected.acquire = protected.acquire.__self__, interrupting(protected.acquire)
    with pytest.raises(KeyboardInterrupt, match="True"), protected:  # So taken
        pass
    assert not lock.locked()


def test_entering_leaves_alone_a_lock_whose_wait_for_another_thread_is_interrupted():
    def interrupted():  # As acquire() is, waiting for the thread holding the lock
        raise KeyboardInterrupt

    protected = ConditionProtected(None)
    lock, protected.acquire = protected.acquire.__self__, interrupted
    with concurrent.futures.ThreadPoolExecutor(1) as elsewhere:
        elsewhere.submit(lock.acquire).result()
        with pytest.raises(KeyboardInterrupt), protected:
            pass
        assert elsewhere.submit(lock._recursion_count).result() == 1  # Still held there, once
        elsewhere.submit(lock.release).result()


def test_conditions_interrupted_as_wait_lets_go_still_raise_and_leave_their_locks_free():
    condition = ConditionProtected(None)
    lock = condition._lock
    condition._release_save = interrupting(lock._release_save)  # Interrupted as it returns, having let go
    # RuntimeError: from __exit__ letting go of what's let go already, with the KeyboardInterrupt as its context
    with pytest.raises((KeyboardInterrupt, RuntimeError)), condition:
        condition.wait()
    assert not lock.locked()


def test_condition_protected_has_every_field_threading_condition_sets():
    # Its __init__ sets some itself, rather than calling threading.Condition's, and it forwards the rest to its lock: so a
    # new one there must be added here
    condition = ConditionProtected(None)
    for name in vars(threading.Condition(threading.RLock())):
        assert name in vars(condition) or name in vars(ConditionProtected), name


def test_condition_protected_waits_with_its_lock_held_reentrantly():
    condition, woke = ConditionProtected(None), []

    def wait():
        with condition, condition:  # wait() must let go of both levels, so that notify() below can take the lock
            woke.append(condition.wait(5))
            woke.append(condition._lock._recursion_count())  # And take both back

    waiter = threading.Thread(target=wait)
    waiter.start()
    while len(condition._waiters) == 0:  # Until it's waiting
        time.sleep(0.001)
    with condition:
        assert condition._is_owned() and condition.locked()
        condition.notify()
    waiter.join(5)
    assert woke == [True, 2] and not condition.locked()


def test_condition_protected_holds_its_lock_while_entered():
    protected = ConditionProtected(value := [])
    with protected as entered, concurrent.futures.ThreadPoolExecutor(1) as elsewhere:
        assert entered is value
        assert not elsewhere.submit(protected.acquire, blocking=False).result()  # Other threads wait for it
        with protected as reentered:  # But not its own: it's recursive
            assert reentered is value
    assert not protected.acquire.__self__.locked()


def test_condition_protected_releases_its_lock_as_errors_leave():
    protected = ConditionProtected(None)
    with pytest.raises(KeyError), protected:
        raise KeyError
    assert not protected.acquire.__self__.locked()


def test_condition_protected_values_are_replaced_for_later_entries():
    protected = ConditionProtected(old := object())
    with protected as entered:
        new = object()
        assert protected.replace_value(new) is new
        assert entered is old
    with protected as entered:
        assert entered is new


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork()")
def test_a_forked_process_resets_conditions_other_threads_held_or_waited_on_but_not_its_own():
    held, waited_on, own = ConditionProtected("held"), ConditionProtected(None), ConditionProtected(None)
    holding, release = threading.Event(), threading.Event()

    def hold():
        with held:
            holding.set()
            release.wait(10)

    def wait():
        with waited_on:
            waited_on.wait(10)

    threads = [threading.Thread(target=hold), threading.Thread(target=wait)]
    for thread in threads:
        thread.start()
    holding.wait(2)
    while len(waited_on._waiters) == 0:  # Until wait() is waiting
        time.sleep(0.001)
    own.acquire()  # As a with block on the forking thread would, as the process forks
    with forked() as f:
        if f.child:
            with held as value:  # Rather than waiting for the lock, which a thread that isn't here holds
                assert value == "held"
            assert len(waited_on._waiters) == 0  # Its thread isn't here: notify() mustn't count it
            assert own._is_owned()  # Still this thread's, so it lets go as usual
            own.release()
            assert not own._lock.locked()
    own.release()
    release.set()
    with waited_on:
        waited_on.notify_all()
    for thread in threads:
        thread.join(2)


def test_only_coroutine_functions_are_valid():
    @define
    class Holder:
        run: object = field(validator=validate_coroutine_function)

    async def run(): ...

    assert Holder(run).run is run
    for invalid in (lambda: run(), None):  # noqa: PLW0108 (Returning a coroutine isn't enough: it must be an async def)
        with pytest.raises(TypeError, match=r"^run must be a coroutine function \(an async def\), not "):
            Holder(invalid)


def test_blocking_checkers_and_their_copies_dont_share_later_adds():
    here = threading.current_thread()
    checker = BlockingChecker()
    copied = checker.copy(add=[AnyEventLoop])  # Outside any loop, here: so refusing nothing
    checker.add(here)
    copied.check("future")
    with pytest.raises(WouldBlock, match="'future' would deadlock"):
        checker.check("future")
    copied.add(here)
    with pytest.raises(WouldBlock):
        copied.check("future")


def test_blocking_checkers_let_go_of_what_has_gone_as_they_add():
    checker = BlockingChecker([gone := threading.Thread(target=print)])
    del gone
    gc.collect()
    checker.check("future")  # Refusing nothing, once gone
    checker.add(here := threading.current_thread())
    assert checker._environments == (weakref.ref(here),)
