"""Tests for async_anywhere._util's helpers."""
from __future__ import annotations

import concurrent.futures
import sys

import pytest
from attrs import define, field

from async_anywhere._util import LockProtected, copy_exception, validate_coroutine_function


def test_copies_keep_the_originals_state():
    with pytest.raises(SystemExit) as raised:
        try:
            raise KeyError("context")
        except KeyError:
            raise SystemExit(3) from ValueError("cause")

    original = raised.value
    original.add_note("note")
    copy = copy_exception(original)
    assert copy is not original and type(copy) is SystemExit and copy.code == 3  # Builtins' own state too
    assert copy.__traceback__ is original.__traceback__ and copy.__notes__ == ["note"]
    assert (copy.__cause__, copy.__context__) == (original.__cause__, original.__context__) and copy.__suppress_context__
    with pytest.raises(SystemExit) as raised:
        sys.exit(3)
    assert not copy_exception(raised.value).__suppress_context__


def test_raising_a_copy_leaves_the_original_alone():
    with pytest.raises(SystemExit) as raised:
        sys.exit(3)
    original = raised.value
    copy, traceback = copy_exception(original), original.__traceback__
    with pytest.raises(SystemExit):
        try:
            raise KeyError("elsewhere")
        except KeyError:
            raise copy
    copy.add_note("note")
    assert copy.__traceback__ is not traceback and type(copy.__context__) is KeyError
    assert original.__traceback__ is traceback and original.__context__ is None and not hasattr(original, "__notes__")


def test_copies_are_raised_from_the_causes_given():
    first, last, root = KeyError("first"), OSError("last"), TypeError("root")
    last.__cause__ = root
    copy = copy_exception(ValueError("error"), first, last)
    assert type(copy.__cause__) is KeyError and copy.__cause__ is not first and first.__cause__ is None  # Unchanged
    assert copy.__cause__.__cause__ is last and last.__cause__ is root  # The last kept as it is, with its own cause
    assert copy.__suppress_context__


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


def test_only_coroutine_functions_are_valid():
    @define
    class Holder:
        run: object = field(validator=validate_coroutine_function)

    async def run(): ...

    assert Holder(run).run is run
    for invalid in (lambda: run(), None):  # noqa: PLW0108 (Returning a coroutine isn't enough: it must be an async def)
        with pytest.raises(TypeError, match=r"^run must be a coroutine function \(an async def\), not "):
            Holder(invalid)
