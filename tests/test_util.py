"""Tests for async_anywhere._util's helpers."""
from __future__ import annotations

import concurrent.futures
import re
import signal
import sys

import pytest
from attrs import define, field

from async_anywhere._util import BlockingSignals, LockProtected, copy_exception, validate_coroutine_function

POSIX_ONLY = pytest.mark.skipif(not hasattr(signal, "SIGUSR1"), reason="Uses SIGUSR1 and SIGUSR2")


@pytest.fixture
def usr_handlers():
    """Installs a handler for SIGUSR1 and SIGUSR2 that the test gives, for the test, then restores what was there."""
    previous = {signum: signal.getsignal(signum) for signum in (signal.SIGUSR1, signal.SIGUSR2)}

    def install(handler):
        for signum in previous:
            signal.signal(signum, handler)

    yield install
    for signum, handler in previous.items():
        signal.signal(signum, handler)


@POSIX_ONLY
def test_blocked_signals_are_raised_once_each_as_the_call_returns_in_the_order_given(usr_handlers):
    handled = []
    usr_handlers(lambda signum, _: handled.append(signum))
    handler = signal.getsignal(signal.SIGUSR1)

    def raises_them():
        for signum in signal.SIGUSR1, signal.SIGUSR2, signal.SIGUSR1:
            signal.raise_signal(signum)
        assert handled == []
        return "returned"

    assert BlockingSignals(["SIGUSR2", signal.SIGUSR1], raises_them)() == "returned"  # By name, or number
    assert handled == [signal.SIGUSR2, signal.SIGUSR1]
    assert signal.getsignal(signal.SIGUSR1) is handler is signal.getsignal(signal.SIGUSR2)  # Restored


@POSIX_ONLY
def test_a_blocked_signals_handler_raising_doesnt_keep_the_next_from_running(usr_handlers):
    handled = []

    def handler(signum, _):
        handled.append(signum)
        raise (KeyError if signum == signal.SIGUSR1 else ValueError)(signum)

    usr_handlers(handler)

    def raises_them():
        signal.raise_signal(signal.SIGUSR2)
        signal.raise_signal(signal.SIGUSR1)

    with pytest.raises(ValueError) as raised:
        BlockingSignals([signal.SIGUSR1, signal.SIGUSR2], raises_them)()
    assert handled == [signal.SIGUSR1, signal.SIGUSR2]
    assert type(raised.value.__context__) is KeyError  # As with CPython's own pending signals
    assert signal.getsignal(signal.SIGUSR1) is handler is signal.getsignal(signal.SIGUSR2)  # Restored


@POSIX_ONLY
def test_a_handler_installed_for_a_blocked_signal_is_kept(usr_handlers):
    usr_handlers(signal.SIG_IGN)

    def installed(signum, _): ...

    BlockingSignals([signal.SIGUSR1, signal.SIGUSR2], lambda: signal.signal(signal.SIGUSR1, installed))()
    assert signal.getsignal(signal.SIGUSR1) is installed and signal.getsignal(signal.SIGUSR2) is signal.SIG_IGN


@pytest.mark.parametrize(
    "value",
    ["SIGNOPE", 0, -1, None, *(getattr(signal, name) for name in ("SIGKILL", "SIGSTOP") if hasattr(signal, name))],
)
def test_signals_that_cant_be_blocked_are_refused(value):
    with pytest.raises(ValueError, match=re.escape(repr(value))):
        BlockingSignals([signal.SIGINT, value], print)


def test_blocking_no_signals_blocks_nothing():
    reached = []

    def interrupted():
        signal.raise_signal(signal.SIGINT)
        reached.append("after")

    with pytest.raises(KeyboardInterrupt):
        BlockingSignals([], interrupted)()
    assert reached == []


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


def test_lock_protected_releases_its_lock_as_errors_leave():
    protected = LockProtected(None)
    with pytest.raises(KeyError), protected:
        raise KeyError
    assert not protected._acquire.__self__.locked()


def test_lock_protected_values_are_replaced_for_later_entries():
    protected = LockProtected(old := object())
    with protected as entered:
        protected.replace_value(new := object())
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
