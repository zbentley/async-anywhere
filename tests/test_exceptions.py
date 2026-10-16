"""Tests for the helpers that assemble CaptiveWorker's exceptions."""
from __future__ import annotations

import sys

import pytest

from async_anywhere.captive._exceptions import copied


def test_copies_keep_the_originals_state():
    with pytest.raises(SystemExit) as raised:
        try:
            raise KeyError("context")
        except KeyError:
            raise SystemExit(3) from ValueError("cause")

    original = raised.value
    original.add_note("note")
    copy = copied(original)
    assert copy is not original and type(copy) is SystemExit and copy.code == 3  # Builtins' own state too
    assert copy.__traceback__ is original.__traceback__ and copy.__notes__ == ["note"]
    assert (copy.__cause__, copy.__context__) == (original.__cause__, original.__context__) and copy.__suppress_context__
    with pytest.raises(SystemExit) as raised:
        sys.exit(3)
    assert not copied(raised.value).__suppress_context__


def test_raising_a_copy_leaves_the_original_alone():
    with pytest.raises(SystemExit) as raised:
        sys.exit(3)
    original = raised.value
    copy, traceback = copied(original), original.__traceback__
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
    copy = copied(ValueError("error"), first, last)
    assert type(copy.__cause__) is KeyError and copy.__cause__ is not first and first.__cause__ is None  # Unchanged
    assert copy.__cause__.__cause__ is last and last.__cause__ is root  # The last kept as it is, with its own cause
    assert copy.__suppress_context__
