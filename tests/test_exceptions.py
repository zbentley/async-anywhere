"""async_anywhere.exceptions: the library's own exceptions, which all mix in AsyncAnywhereException, and which can be
imported without importing the rest of the library."""
from __future__ import annotations

import asyncio
import copy
import os
import pickle
import subprocess
import sys
from abc import ABC
from typing import Final

import pytest

import async_anywhere
from async_anywhere import exceptions
from async_anywhere.exceptions import (
    AsyncAnywhereException,
    CleanStop,
    StateTrackerError,
    EventLoopExecutorForked,
    JobCancelledByExecutorShutdown,
    EventLoopExecutorStopped,
    WouldBlock,
)
from async_anywhere.futures import AwaitableFuture
from async_anywhere.state_tracker import State, StateTracker


class Tracker(StateTracker):
    Only: Final[State] = State(initial=True)


EXCEPTIONS = {
    "WouldBlock": lambda: WouldBlock(AwaitableFuture(), "a loop"),
    "StateTrackerError": lambda: StateTrackerError(Tracker(), "Could not transition to state", Tracker.Only),
    "EventLoopExecutorStopped": lambda: EventLoopExecutorStopped("(idle) manually stopped", KeyError("why")),
    "CleanStop": lambda: CleanStop("(idle) expired"),
    "EventLoopExecutorForked": EventLoopExecutorForked,
    "JobCancelledByExecutorShutdown": lambda: JobCancelledByExecutorShutdown.from_cancellation(asyncio.CancelledError(), CleanStop("stopped")),
}


def test_importing_it_imports_no_other_part_of_the_library():
    # In a fresh interpreter, as this one has imported the rest already
    code = "import sys, async_anywhere.exceptions; print(*sorted(m for m in sys.modules if 'async_anywhere' in m))"
    src = os.path.dirname(os.path.dirname(async_anywhere.__file__))
    ran = subprocess.run(
        [sys.executable, "-c", code], env={**os.environ, "PYTHONPATH": src}, capture_output=True, text=True, check=True
    )
    assert ran.stdout.split() == ["async_anywhere", "async_anywhere.exceptions"]



def test_only_exceptions_we_never_instantiate_are_marked_abstract_and_the_rest_are_tested_below():
    marked = {name for name in exceptions.__all__ if ABC in getattr(exceptions, name).__bases__}
    assert marked == {"AsyncAnywhereException", "EventLoopExecutorBroken"}
    assert set(exceptions.__all__) - marked == EXCEPTIONS.keys()


@pytest.mark.parametrize("make", EXCEPTIONS.values(), ids=EXCEPTIONS.keys())
def test_each_is_ours_and_copies(make):
    error, cause = make(), OSError("cause")
    error.add_note("a note")
    assert isinstance(error, AsyncAnywhereException)
    for duplicate in (copy.copy(error), pickle.loads(pickle.dumps(error)), error.copy(), error.copy(cause)):
        assert type(duplicate) is type(error) and duplicate is not error
        assert duplicate.args == error.args and duplicate.__notes__ == ["a note"]
    assert error.copy().__cause__ is error.__cause__ and error.copy(cause).__cause__ is cause


def test_copies_keep_the_originals_state():
    with pytest.raises(SystemExit) as raised:
        try:
            raise KeyError("context")
        except KeyError:
            raise SystemExit(3) from ValueError("cause")

    original = raised.value
    original.add_note("note")
    copy = AsyncAnywhereException.copy_exception(original)
    assert copy is not original and type(copy) is SystemExit and copy.code == 3  # Builtins' own state too
    assert copy.__traceback__ is original.__traceback__ and copy.__notes__ == ["note"]
    assert (copy.__cause__, copy.__context__) == (original.__cause__, original.__context__) and copy.__suppress_context__
    with pytest.raises(SystemExit) as raised:
        sys.exit(3)
    assert not AsyncAnywhereException.copy_exception(raised.value).__suppress_context__


def test_raising_a_copy_leaves_the_original_alone():
    with pytest.raises(SystemExit) as raised:
        sys.exit(3)
    original = raised.value
    copy, traceback = AsyncAnywhereException.copy_exception(original), original.__traceback__
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
    copy = AsyncAnywhereException.copy_exception(ValueError("error"), first, last)
    assert type(copy.__cause__) is KeyError and copy.__cause__ is not first and first.__cause__ is None  # Unchanged
    assert copy.__cause__.__cause__ is last and last.__cause__ is root  # The last kept as it is, with its own cause
    assert copy.__suppress_context__
