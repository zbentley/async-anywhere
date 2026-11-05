"""Tests for async_anywhere._util's helpers."""
from __future__ import annotations

import concurrent.futures
import gc
import sys
import threading
import weakref

import pytest
from attrs import define, field

from async_anywhere._util import AnyEventLoop, BlockingChecker, LockProtected, validate_coroutine_function
from async_anywhere.exceptions import WouldBlock


@pytest.mark.parametrize("recursive", [False, True])
def test_lock_protected_holds_its_lock_while_entered(recursive):
    protected = LockProtected(value := [], recursive=recursive)
    lock = protected._acquire.__self__  # The lock its bound methods belong to
    with protected as entered, concurrent.futures.ThreadPoolExecutor(1) as elsewhere:
        assert entered is value
        assert not elsewhere.submit(lock.acquire, blocking=False).result()  # Other threads wait for it
        if reentered := lock.acquire(blocking=False):  # As its own does, unless it's recursive
            lock.release()
        assert reentered is recursive
    assert not lock.locked()


def test_lock_protected_releases_its_lock_as_errors_leave():
    protected = LockProtected(None)
    with pytest.raises(KeyError), protected:
        raise KeyError
    assert not protected._acquire.__self__.locked()


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
    copied = checker.copy([AnyEventLoop])  # Outside any loop, here: so refusing nothing
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
