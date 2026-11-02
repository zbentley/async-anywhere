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

import pytest

import async_anywhere
from async_anywhere import exceptions
from async_anywhere.exceptions import (
    AsyncAnywhereException,
    CleanStop,
    StateTrackerError,
    WorkerForked,
    WorkerInterrupted,
    WorkerStopped,
    WouldBlock,
)
from async_anywhere.futures import AwaitableFuture
from async_anywhere.state_tracker import State, StateTracker


class Tracker(StateTracker):
    Only = State(initial=True)


EXCEPTIONS = {
    "WouldBlock": lambda: WouldBlock(AwaitableFuture(), "a loop"),
    "StateTrackerError": lambda: StateTrackerError(Tracker(), "Could not transition to state", Tracker.Only),
    "WorkerStopped": lambda: WorkerStopped("(idle) manually stopped", KeyError("why")),
    "CleanStop": lambda: CleanStop("(idle) expired"),
    "WorkerForked": WorkerForked,
    "WorkerInterrupted": lambda: WorkerInterrupted._from_cancellation(asyncio.CancelledError(), CleanStop("stopped")),
}


def test_importing_it_imports_no_other_part_of_the_library():
    # In a fresh interpreter, as this one has imported the rest already
    code = "import sys, async_anywhere.exceptions; print(*sorted(m for m in sys.modules if 'async_anywhere' in m))"
    src = os.path.dirname(os.path.dirname(async_anywhere.__file__))
    ran = subprocess.run(
        [sys.executable, "-c", code], env={**os.environ, "PYTHONPATH": src}, capture_output=True, text=True, check=True
    )
    assert ran.stdout.split() == ["async_anywhere", "async_anywhere.exceptions"]


def test_the_mixin_isnt_an_exception_itself():
    assert not issubclass(AsyncAnywhereException, BaseException)


def test_only_exceptions_we_never_instantiate_are_marked_abstract_and_the_rest_are_tested_below():
    marked = {name for name in exceptions.__all__ if ABC in getattr(exceptions, name).__bases__}
    assert marked == {"AsyncAnywhereException", "WorkerBroken"}
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
