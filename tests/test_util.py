"""Tests for async_anywhere._util's helpers."""
from __future__ import annotations

import concurrent.futures
import gc
import sys
import threading
import weakref

import pytest
from attrs import define, field

from async_anywhere._util import AnyEventLoop, BlockingChecker, Condition, LockProtected, validate_coroutine_function
from async_anywhere.exceptions import WouldBlock


def interrupting(method):
    """Wraps method, raising KeyboardInterrupt (with what it returned) as it returns, as a signal handler can."""

    def interrupted(*args):
        raise KeyboardInterrupt(method(*args))

    return interrupted


def test_entering_lets_go_of_a_lock_taken_as_a_signal_handler_raises():
    protected = LockProtected(None)
    lock, protected.acquire = protected.acquire.__self__, interrupting(protected.acquire)
    with pytest.raises(KeyboardInterrupt, match="True"), protected:  # So taken
        pass
    assert not lock.locked()


def test_entering_leaves_alone_a_lock_whose_wait_for_another_thread_is_interrupted():
    def interrupted():  # As acquire() is, waiting for the thread holding the lock
        raise KeyboardInterrupt

    protected = LockProtected(None)
    lock, protected.acquire = protected.acquire.__self__, interrupted
    with concurrent.futures.ThreadPoolExecutor(1) as elsewhere:
        elsewhere.submit(lock.acquire).result()
        with pytest.raises(KeyboardInterrupt), protected:
            pass
        assert elsewhere.submit(lock._recursion_count).result() == 1  # Still held there, once
        elsewhere.submit(lock.release).result()


def test_conditions_interrupted_as_wait_lets_go_still_raise_and_leave_their_locks_free():
    condition = Condition()
    lock = condition._lock
    condition._release_save = interrupting(lock._release_save)  # Interrupted as it returns, having let go
    # RuntimeError: from __exit__ letting go of what's let go already, with the KeyboardInterrupt as its context
    with pytest.raises((KeyboardInterrupt, RuntimeError)), condition:
        condition.wait()
    assert not lock.locked()


def test_lock_protected_holds_its_lock_while_entered():
    protected = LockProtected(value := [])
    with protected as entered, concurrent.futures.ThreadPoolExecutor(1) as elsewhere:
        assert entered is value
        assert not elsewhere.submit(protected.acquire, blocking=False).result()  # Other threads wait for it
        with protected as reentered:  # But not its own: it's recursive
            assert reentered is value
    assert not protected.acquire.__self__.locked()


def test_lock_protected_releases_its_lock_as_errors_leave():
    protected = LockProtected(None)
    with pytest.raises(KeyError), protected:
        raise KeyError
    assert not protected.acquire.__self__.locked()


def test_lock_protected_values_are_replaced_for_later_entries():
    protected = LockProtected(old := object())
    with protected as entered:
        new = object()
        assert protected.replace_value(new) is new
        assert entered is old
    with protected as entered:
        assert entered is new


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
