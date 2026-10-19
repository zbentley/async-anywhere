"""Every CaptiveWorker test runs with workers on asyncio's own loop, and on uvloop's: see worker_loop()."""
from __future__ import annotations

import pytest
import uvloop

from async_anywhere.captive import CaptiveWorker


@pytest.fixture(autouse=True, params=["asyncio", "uvloop"], ids=lambda name: f"worker-{name}")
def worker_loop(request, monkeypatch):
    """The loop workers run on, unless a test gives them a loop_factory: asyncio's own, or uvloop's."""
    if request.param == "uvloop":
        # The default attrs bakes into CaptiveWorker's __init__(), so every worker gets it, however it's constructed
        monkeypatch.setitem(CaptiveWorker.__init__.__kwdefaults__, "loop_factory", uvloop.new_event_loop)
    return request.param
