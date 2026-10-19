"""async_anywhere without trio installed, which it uses only if it is: in a subprocess, as importing can't be undone."""
from __future__ import annotations

import ast
import pkgutil
import subprocess
import sys
import textwrap
from pathlib import Path

import async_anywhere


# Every module imported, then awaiting on asyncio, which is what asks whether it's in a trio task
SCRIPT = textwrap.dedent(
    """
    import sys
    sys.modules["trio"] = None  # So that importing it raises ModuleNotFoundError, as if it weren't installed
    sys.path.insert(0, sys.argv[1])  # The source the tests import
    import asyncio, importlib, pkgutil, threading
    import async_anywhere
    modules = sorted(module.name for module in pkgutil.walk_packages(async_anywhere.__path__, "async_anywhere."))
    for name in modules:
        importlib.import_module(name)
    from async_anywhere.captive import CaptiveWorker
    from async_anywhere.futures import AwaitableFuture

    async def main():
        pending = AwaitableFuture()
        threading.Timer(0.01, pending.set_result, ("awaited",)).start()
        worker = CaptiveWorker()

        async def job():
            return "bridged"

        return await pending, await worker.bridge(job)(), type(await worker.stop()).__name__

    print(repr((modules, asyncio.run(main()))))
    """
)


def test_everything_imports_and_works_on_asyncio_without_trio():
    source = str(Path(async_anywhere.__file__).parents[1])
    ran = subprocess.run(  # noqa: S603 (This interpreter, running the script above)
        [sys.executable, "-W", "error", "-c", SCRIPT, source], capture_output=True, text=True, timeout=30, check=False
    )
    assert ran.returncode == 0 and not ran.stderr, ran.stderr
    modules, results = ast.literal_eval(ran.stdout)
    assert modules == sorted(module.name for module in pkgutil.walk_packages(async_anywhere.__path__, "async_anywhere."))
    assert "async_anywhere._util.trio" in modules
    assert results == ("awaited", "bridged", "CleanStop")
