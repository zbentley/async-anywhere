"""Suite-wide options, the backends async tests run on (see pytest_generate_tests()), and the strict teardown every test
gets: see strict_teardown()."""
from __future__ import annotations

import gc
import logging
import os
import sys
from collections.abc import Callable
from contextlib import suppress
from weakref import WeakSet

import pytest
import uvloop
from _pytest.threadexception import ThreadExceptionMeta, thread_exceptions as thread_exceptions_key

from async_anywhere.executors import _fork_safety


pytest.register_assert_rewrite("tests.test_executors.utils")  # Before test modules import it


def pytest_addoption(parser):
    parser.addoption(
        "--torture",
        action="store_true",
        help="Repeat each test 50 times (unless --count is given), scale up tests' inline loops and thread fan-outs (see "
        "the scale fixture), and switch threads as often as the interpreter can",
    )
    parser.addoption(
        "--asyncio-debug-all",
        action="store_true",
        help="Run every event loop, workers' and tests' alike, in asyncio's debug mode",
    )


def pytest_configure(config):
    if config.getoption("torture"):
        if config.option.count == 1:  # pytest-repeat's default
            config.option.count = 50
        sys.setswitchinterval(1e-6)
    if config.getoption("asyncio_debug_all"):
        os.environ["PYTHONASYNCIODEBUG"] = "1"  # Read by each loop as it's made, as asyncio.new_event_loop() makes them


def pytest_generate_tests(metafunc):
    # Async tests (which anyio's plugin runs) run on asyncio, with its own loop and with uvloop's, or on the backends
    # their backends marker names: trio too, for tests written against anyio's API (or trio's) rather than asyncio's
    if "anyio_backend" in metafunc.fixturenames:
        backends = {
            "asyncio": ("asyncio", {}),
            "uvloop": ("asyncio", {"loop_factory": uvloop.new_event_loop}),
            # A timeout's signal fails a test that hangs, but anyio's trio runner then waits forever for the run to end
            "trio": pytest.param(("trio", {}), marks=pytest.mark.timeout(method="thread")),
        }
        marker = metafunc.definition.get_closest_marker("backends")
        names = marker.args if marker else ("asyncio", "uvloop")
        metafunc.parametrize("anyio_backend", [backends[name] for name in names], ids=names)


@pytest.fixture
def anyio_backend():
    """Overrides anyio's, whose own params would clash with pytest_generate_tests(), which gives this its value."""
    raise AssertionError("anyio_backend is parametrized by pytest_generate_tests()")


@pytest.fixture
def scale(request) -> int:
    """How many times over tests run their inline loops and thread fan-outs: 10 under --torture."""
    return 10 if request.config.getoption("torture") else 1


@pytest.fixture
def thread_exceptions(request) -> Callable[[], list[BaseException | None]]:
    """Drains, when called, the exceptions threads raised in the background, which pytest's threading.excepthook collects, and fails
    the test for, at the end of each phase: unless the test drains them first."""
    queue = request.config.stash[thread_exceptions_key]

    def drain():
        drained = []
        while queue:
            entry = queue.popleft()
            drained.append(entry.exc_value if isinstance(entry, ThreadExceptionMeta) else entry)
        return drained

    return drain


@pytest.fixture(autouse=True)
def strict_teardown(request, caplog):
    """Fails the test if it left a worker running (stopping it, so it can't spill into later tests), or asyncio logged an
    error (unless it's marked logs_asyncio_errors). Collects garbage first, so that finalizers' warnings and asyncio's
    reports of what was never retrieved land on this test. pytest itself fails the test for exceptions threads raised
    that the test didn't drain (see thread_exceptions), and for warnings, which filterwarnings makes errors."""
    before = WeakSet(_fork_safety.workers.instances)
    yield
    problems = []
    for worker in set(_fork_safety.workers.instances) - set(before):
        if worker.state is not worker.Stopped:
            problems.append(f"{worker!r} was left running")
            with suppress(BaseException):
                worker.shutdown(wait=False, cancel_futures=True).result(2)
        worker._thread.join(2)
        if worker._thread.is_alive():
            problems.append(f"{worker!r}'s thread never finished")
    gc.collect()
    if not request.node.get_closest_marker("logs_asyncio_errors"):
        problems.extend(
            f"asyncio logged an error in {when}: {record.getMessage()}"
            for when in ("call", "teardown")
            for record in caplog.get_records(when)
            if record.name == "asyncio" and record.levelno >= logging.ERROR
        )
    if problems:
        pytest.fail("\n".join(problems), pytrace=False)
